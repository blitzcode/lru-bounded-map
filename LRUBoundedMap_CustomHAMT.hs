
{-# LANGUAGE BangPatterns #-}
{-# OPTIONS_GHC -fno-full-laziness -funbox-strict-fields #-}

module LRUBoundedMap_CustomHAMT ( Map
                                , empty
                                , toList
                                , null
                                , size
                                , member
                                , notMember
                                , insert
                                , update
                                , delete
                                , lookup
                                , popOldest
                                , popNewest
                                , valid
                                ) where

import Prelude hiding (lookup, null)
import qualified Data.Hashable as H
import Data.Hashable (Hashable)
import Data.Bits
import Data.Maybe
import Data.Word
import Data.List (find, partition, foldl')
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import Control.Applicative hiding (empty)
import Control.Monad
import Control.Monad.Writer
import Control.DeepSeq (NFData(rnf))

-- Associative array implemented on top of a Hashed Array Mapped Trie (HAMT), based on the
-- implementation in Data.HashMap. Basically a prefix tree over the bits of key hashes, with a
-- higher than binary branching factor. Additional least / most recently used bounds for
-- subtrees are stored so the data structure can have an upper bound on the number of elements
-- and remove the least recently used one overflow. The other bound allows to retrieve the item
-- which was inserted / touched last

data Map k v = Map { mLimit :: !Int
                   , mTick  :: !Word64 -- We use a 'tick', which we keep incrementing, to keep
                                       -- track of how old elements are relative to each other
                   , mSize  :: !Int -- So size is O(1) instead of O(n)
                   , mHAMT  :: !(HAMT k v)
                   }

instance (NFData k, NFData v) => NFData (Map k v) where
    rnf (Map l t s h) = rnf l `seq` rnf t `seq` rnf s `seq` rnf h

type Hash = Word

hash :: H.Hashable a => a -> Hash
hash = fromIntegral . H.hash

data Leaf k v = L !k !v

instance (NFData k, NFData v) => NFData (Leaf k v) where
    rnf (L k v) = rnf k `seq` rnf v

-- Note that we don't have bitmap indexing for partially filled nodes. This simplifies the code,
-- but comes at the expensive of memory usage and access
data HAMT k v = Empty
              | Node !(V.Vector (HAMT k v))
              | Leaf !Hash !(Leaf k v)
              | Collision !Hash ![Leaf k v]

instance (NFData k, NFData v) => NFData (HAMT k v) where
    rnf Empty            = ()
    rnf (Leaf _ l)       = rnf l
    rnf (Node ch)        = rnf ch
    rnf (Collision _ ch) = rnf ch

bitsPerSubkey :: Int
bitsPerSubkey = 4

subkeyMask :: Hash
subkeyMask = (1 `shiftL` bitsPerSubkey) - 1

maxChildren :: Int
maxChildren = 1 `shiftL` bitsPerSubkey

-- Retrieve a leaf child index from a hash and a subkey offset
indexNode :: Hash -> Int -> Int
indexNode h s = fromIntegral $ (h `shiftR` s) .&. subkeyMask

-- Insert a new element into the map, return the new map and the truncated
-- element (if over the limit)
--{-# INLINEABLE insert #-}
insert :: (Eq k, Hashable k) => k -> v -> Map k v -> (Map k v, Maybe (k, v))
insert !k !v !m = insertInternal False k v m

data Pair a b = Pair !a !b

--{-# INLINE insertInternal #-}
insertInternal :: (Eq k, Hashable k) => Bool -> k -> v -> Map k v -> (Map k v, Maybe (k, v))
insertInternal !updateOnly !kIns !vIns !m =
    let go !h !k !v !_ Empty = if   updateOnly
                           then Pair Empty False -- We're in update mode, no insert
                           else Pair (Leaf h (L k v)) True 
        go !h !k !v !s !t@(Leaf !lh !li@(L !lk !lv)) =
            if   h == lh
            then if   k == lk
                 then Pair (Leaf h (L k v)) False -- Update value
                 else -- We have a hash collision, insert
                      if   updateOnly -- ...unless we're in update mode
                      then Pair t False
                      else Pair (Collision h [L k v, li]) True
            else -- Expand leaf into interior node
                 if   updateOnly
                 then Pair t False
                 else let !ia           = indexNode h  s
                          !ib           = indexNode lh s
                      in Pair ( Node $! V.create $ do
                                    vec <- VM.replicate maxChildren Empty
                                    if   ia /= ib -- Subkey collision?
                                    then do VM.write vec ia $! Leaf h (L k v)
                                            VM.write vec ib t
                                    else do -- Collision, add one level
                                            let !(Pair subtree _) = go h k v (s + bitsPerSubkey) t
                                            VM.write vec ia $! subtree
                                    return vec
                              ) True
        go !h !k !v !s !t@(Node ch) =
            let !idx           = indexNode h s
                !subtree       = ch `V.unsafeIndex` idx 
                !(Pair subtree' i) = -- Traverse into child with matching subkey
                                  go h k v (s + bitsPerSubkey) subtree
            in  subtree' `seq` i `seq` Pair (Node $! ch V.// [(idx, subtree')]) i
        go !h !k !v !s !t@(Collision colh ch) =
            if   updateOnly
            then if   h == colh
                 then let traverseUO [] = [] -- No append in update mode
                          traverseUO (l@(L lk lv):xs) =
                               if   lk == k
                               then L k v : xs
                               else l : traverseUO xs
                      in Pair (Collision h $! traverseUO ch) False
                 else Pair t False
            else if   h == colh
                 then let traverse [] = [L k v] -- Append new leaf
                          traverse (l@(L lk lv):xs) =
                               if   lk == k
                               then L k v : xs -- Update value
                               else l : traverse xs
                      in  Pair (Collision h $! traverse ch)
                               (length ch /= length (traverse ch)) -- TODO: Slow
                 else -- Expand collision into interior node
                      go h k v s . Node $! V.create $ do
                          vec <- VM.replicate maxChildren Empty
                          VM.write vec (indexNode colh s) t
                          return vec
        !(Pair m' i') = go (hash kIns) kIns vIns 0 $ mHAMT m
    in  m' `seq` i' `seq` mSize m `seq` ( m { mHAMT = m'
            , mSize = mSize m + if i' then 1 else 0
            }
        , Nothing
        )

empty :: Int -> Map k v
empty limit | limit >= 1 = Map { mLimit = limit
                               , mTick  = 0
                               , mSize  = 0
                               , mHAMT  = Empty
                               }
            | otherwise  = error "limit for LRUBoundedMap needs to be >= 1"

{-# INLINEABLE size #-}
size :: Map k v -> (Int, Int)
size m = (mSize m {-sizeTraverse m-}, mLimit m)

-- O(n) size-by-traversal
sizeTraverse :: Map k v -> Int
sizeTraverse m = go 0 $ mHAMT m
    where go n Empty            = n
          go n (Leaf _ _)       = n + 1
          go n (Node ch)        = V.foldl' (go) n ch
          go n (Collision _ ch) = n + length ch

{-# INLINEABLE null #-}
null :: Map k v -> Bool
null m = case mHAMT m of Empty -> True; _ -> False

{-# INLINEABLE member #-}
member :: (Eq k, Hashable k) => k -> Map k v -> Bool
member k = undefined

{-# INLINEABLE notMember #-}
notMember :: (Eq k, Hashable k) => k -> Map k v -> Bool
notMember k m = not $ member k m

toList :: Map k v -> [(k, v)]
toList m =  go [] $ mHAMT m
    where go l Empty            = l
          go l (Leaf _ (L k v)) = (k, v) : l
          go l (Node ch)        = V.foldl' (\l' n -> go l' n) l ch
          go l (Collision _ ch) = foldl' (\l' (L k v) -> (k, v) : l') l ch
{-# INLINEABLE toList #-}

-- Lookup element, also update LRU
{-# INLINEABLE lookup #-}
lookup :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
lookup k' m = (m, go (hash k') k' 0 $ mHAMT m)
    where go !_ !_ !_ Empty = Nothing
          go h k _ (Leaf lh (L lk lv))
              | lh /= h   = Nothing
              | lk /= k   = Nothing
              | otherwise = Just lv
          go h k s (Node ch) = go h k (s + bitsPerSubkey) (ch `V.unsafeIndex` indexNode h s)
          go h k _ (Collision colh ch)
              | colh == h = (\(L _ lv) -> lv) <$> find (\(L lk _) -> lk == k) ch
              | otherwise = Nothing

{-# INLINEABLE delete #-}
delete :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
delete k' m =
    let go !_ !_ !_ Empty = (Empty, Nothing)
        go h k _ t@(Leaf lh (L lk lv))
            | lh /= h   = (t, Nothing)
            | lk /= k   = (t, Nothing)
            | otherwise = (Empty, Just lv)
        go h k s t@(Node ch) =
            let !idx              = indexNode h s
                !subtree          = ch `V.unsafeIndex` idx
                !(subtree', del') = go h k (s + bitsPerSubkey) subtree
                !ch'              = ch V.// [(idx, subtree')]
                !used             = -- Non-empty slots in the updated child vector
                                    V.ifoldr (\i t' u -> case t' of Empty -> u; _ -> i : u) [] ch'
            in  case used of
                []     -> (Empty, del') -- We removed the last element, delete node
                (x:[]) -> -- If we deleted our second last element, we
                          -- also need to check whether the last child
                          -- is a leaf / collision
                          let !lst = ch' `V.unsafeIndex` x in case lst of
                              Leaf _ _      -> (lst, del') -- Replace node by leaf
                              Collision _ _ -> (lst, del') -- ...
                              _             -> (Node ch', del')
                _      -> (Node ch', del')
        go h k _ t@(Collision colh ch)
            | colh == h = let (delch', ch') = partition (\(L lk _) -> lk == k) ch
                          in  if   length ch' == 1
                              then  -- Deleted last remaining collision, it's a leaf node now
                                   (Leaf h $ head ch', Just $ (\((L _ lv):[]) -> lv) delch')
                              else (Collision h ch', (\(L _ lv) -> lv) <$> listToMaybe delch')
            | otherwise = (t, Nothing)
        !(m', del) = go (hash k') k' 0 $ mHAMT m
    in  ( m { mHAMT = m'
            , mSize = mSize m - if isJust del then 1 else 0
            }
        , del
        )

-- Delete and return most recently used item
popNewest :: (Eq k, Hashable k) => Map k v -> (Map k v, Maybe (k, v))
popNewest m = undefined

-- Delete and return least recently used item
popOldest :: (Eq k, Hashable k) => Map k v -> (Map k v, Maybe (k, v))
popOldest m = undefined

{-# INLINEABLE update #-}
update :: (Eq k, Hashable k) => k -> v -> Map k v -> Map k v
update k v m =
    case insertInternal True k v m of
        (m', Nothing) -> m'
        _             -> error "LRUBoundedMap.update: insertInternal truncated with updateOnly"

valid :: (Eq k, Hashable k, Eq v) => Map k v -> Maybe String
valid m =
    let w =
         execWriter $ do
             when (mLimit m < 1) $
                 tell "Invalid limit (< 1)\n"
             when ((fst $ size m) /= sizeTraverse m) $
                 tell "Mismatch beween cached and actual size\n"
             --when ((fst $ size m) > mLimit m)
               --  $ tell "Size over the limit\n"
             let traverse s t =
                   case t of
                       Leaf h (L k v) -> checkKey h k v
                       Collision h ch -> do
                           when (length ch < 2) $
                               tell "Hash collision node with <2 children\n"
                           forM_ ch $ \(L lk lv) -> checkKey h lk lv
                       Node ch -> do
                           let used =
                                 V.ifoldr (\i t' u -> case t' of Empty -> u; _ -> i : u) [] ch
                           when (s + bitsPerSubkey > bitSize (undefined :: Word)) $
                               tell "Subkey shift too large during traversal\n"
                           when (V.length ch /= maxChildren) $
                               tell "Node with a child vector /= maxChildren\n"
                           when (length used == 0) $
                               tell "Node with only empty children\n"
                           when (length used == 1) $
                              case ch V.! head used of
                                  Leaf      _ _ -> tell "Node with single Leaf child\n"
                                  Collision _ _ -> tell "Node with single Collision child\n"
                                  _             -> return ()
                           forM_ (V.toList ch) $ traverse (s + bitsPerSubkey)
                       Empty -> return ()
                 checkKey h k v = do
                     when (hash k /= h) $
                         tell "Hash / key mismatch\n"
                     case snd $ lookup k m of
                         Nothing ->
                             tell "Can't lookup key found during traversal\n"
                         Just v' -> when (v /= v') .
                             tell $ "Lookup of key found during traversal yields " ++
                                    "different value\n"
                     let (m', v') = delete k m
                     when ((fst $ size m') /= (fst $ size m) - 1) $
                         tell "Deleting key did not reduce size\n"
                     when (fromMaybe v v' /= v) $
                         tell "Delete returned wrong value\n"
              in traverse 0 $ mHAMT m
             let keysL      = map (fst) $ toList m
                 allDeleted = foldl' (\r k -> fst $ delete k r) m keysL
             when (length keysL /= (fst $ size m)) $
                 tell "Length of toList does not match size\n"
             unless (null allDeleted) $
                 tell "Deleting all elements does not result in an empty map\n"
             unless ((fst $ size allDeleted) == 0) $
                 tell "Deleting all elements does not result in a zero size map\n"
    in  case w of [] -> Nothing
                  xs -> Just xs

