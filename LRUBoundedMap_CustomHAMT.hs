
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
                                , lookupNoLRU
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
import Data.Foldable (minimumBy, maximumBy)
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
--
-- TODO: LRU / bounded aspect not working yet, about 50% done


data Map k v = Map { mLimit :: !Int
                   , mTick  :: !Word64 -- We use a 'tick', which we keep incrementing, to keep
                                       -- track of how old elements are relative to each other
                   , mSize  :: !Int -- Cached to make size O(1) instead of O(n)
                   , mHAMT  :: !(HAMT k v)
                   }

instance (NFData k, NFData v) => NFData (Map k v) where
    rnf (Map l t s h) = rnf l `seq` rnf t `seq` rnf s `seq` rnf h

type Hash = Word

{-# INLINE hash #-}
hash :: H.Hashable a => a -> Hash
hash = fromIntegral . H.hash

data Leaf k v = L !k !v !Word64 -- LRU tick

instance (NFData k, NFData v) => NFData (Leaf k v) where
    rnf (L k v t) = rnf k `seq` rnf v

data OldNew = OldNew !Int !Int

-- Note that we don't have bitmap indexing for partially filled nodes. This simplifies the code,
-- but comes at the expense of memory usage and access
data HAMT k v = Empty
              | Node !OldNew !(V.Vector (HAMT k v))
              | Leaf !Hash !(Leaf k v)
              | Collision !Hash ![Leaf k v]

instance (NFData k, NFData v) => NFData (HAMT k v) where
    rnf Empty            = ()
    rnf (Leaf _ l)       = rnf l
    rnf (Node m ch)      = m `seq` rnf ch
    rnf (Collision _ ch) = rnf ch

{-# INLINE bitsPerSubkey #-}
bitsPerSubkey :: Int
bitsPerSubkey = 4

{-# INLINE subkeyMask #-}
subkeyMask :: Hash
subkeyMask = (1 `shiftL` bitsPerSubkey) - 1

{-# INLINE maxChildren #-}
maxChildren :: Int
maxChildren = 1 `shiftL` bitsPerSubkey

-- Retrieve a leaf child index from a hash and a subkey offset
{-# INLINE indexNode #-}
indexNode :: Hash -> Int -> Int
indexNode h s = fromIntegral $ (h `shiftR` s) .&. subkeyMask

-- Insert a new element into the map, return the new map and the truncated
-- element (if over the limit)
{-# INLINEABLE insert #-}
insert :: (Eq k, Hashable k) => k -> v -> Map k v -> (Map k v, Maybe (k, v))
insert !k !v !m = insertInternal False k v m

data Pair a b = Pair !a !b

-- TODO: Made a terrible mess out of this function, split into insert / update case,
--       remove most of the strictness annotations, lots of optimization potential
-- TODO: No LRU update implemented
{-# INLINE insertInternal #-}
insertInternal :: (Eq k, Hashable k) => Bool -> k -> v -> Map k v -> (Map k v, Maybe (k, v))
insertInternal !updateOnly !kIns !vIns !m =
    let go !h !k !v !_ Empty = if   updateOnly
                           then Pair Empty False -- We're in update mode, no insert
                           else Pair (Leaf h $ L k v tick) True
        go !h !k !v !s !t@(Leaf !lh !li@(L !lk !lv !lt)) =
            if   h == lh
            then if   k == lk
                 then Pair (Leaf h $ L k v tick) False -- Update value
                 else -- We have a hash collision, insert
                      if   updateOnly -- ...unless we're in update mode
                      then Pair t False
                      else Pair (Collision h [L k v tick, li]) True
            else -- Expand leaf into interior node
                 if   updateOnly
                 then Pair t False
                 else let !ia           = indexNode h  s
                          !ib           = indexNode lh s
                      in Pair ( Node (OldNew 0 0) $! V.create $ do
                                    vec <- VM.replicate maxChildren Empty
                                    if   ia /= ib -- Subkey collision?
                                    then do VM.write vec ia $! Leaf h (L k v tick)
                                            VM.write vec ib t
                                    else do -- Collision, add one level
                                            let !(Pair subtree _) = go h k v (s + bitsPerSubkey) t
                                            VM.write vec ia $! subtree
                                    return vec
                              ) True
        go !h !k !v !s !t@(Node _ ch) =
            let !idx           = indexNode h s
                !subtree       = ch `V.unsafeIndex` idx
                !(Pair subtree' i) = -- Traverse into child with matching subkey
                                  go h k v (s + bitsPerSubkey) subtree
            in  subtree' `seq` i `seq` Pair (Node (OldNew 0 0) $! ch V.// [(idx, subtree')]) i
        go !h !k !v !s !t@(Collision colh ch) =
            if   updateOnly
            then if   h == colh
                 then let traverseUO [] = [] -- No append in update mode
                          traverseUO (l@(L lk lv _):xs) =
                               if   lk == k
                               then L k v tick : xs
                               else l : traverseUO xs
                      in Pair (Collision h $! traverseUO ch) False
                 else Pair t False
            else if   h == colh
                 then let traverse [] = [L k v tick] -- Append new leaf
                          traverse (l@(L lk lv _):xs) =
                               if   lk == k
                               then L k v tick : xs -- Update value
                               else l : traverse xs
                      in  Pair (Collision h $! traverse ch)
                               (length ch /= length (traverse ch)) -- TODO: Slow
                 else -- Expand collision into interior node
                      go h k v s . Node (OldNew 0 0) $! V.create $ do
                          vec <- VM.replicate maxChildren Empty
                          VM.write vec (indexNode colh s) t
                          return vec
        !(Pair m' i') = go (hash kIns) kIns vIns 0 $ mHAMT m
        !tick = mTick m
    in  m' `seq` i' `seq` mSize m `seq`
        ( m { mHAMT = m'
            , mSize = mSize m + if i' then 1 else 0
            , mTick = tick + 1
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
size m = (mSize m, mLimit m)

-- O(n) size-by-traversal
sizeTraverse :: Map k v -> Int
sizeTraverse m = go 0 $ mHAMT m
    where go n Empty            = n
          go n (Leaf _ _)       = n + 1
          go n (Node _ ch)      = V.foldl' (go) n ch
          go n (Collision _ ch) = n + length ch

{-# INLINEABLE null #-}
null :: Map k v -> Bool
null m = case mHAMT m of Empty -> True; _ -> False

{-# INLINEABLE member #-}
member :: (Eq k, Hashable k) => k -> Map k v -> Bool
member k m = isJust . snd $ lookup k m

{-# INLINEABLE notMember #-}
notMember :: (Eq k, Hashable k) => k -> Map k v -> Bool
notMember k m = not $ member k m

{-# INLINEABLE toList #-}
toList :: Map k v -> [(k, v)]
toList m = go [] $ mHAMT m
    where go l Empty              = l
          go l (Leaf _ (L k v _)) = (k, v) : l
          go l (Node _ ch)        = V.foldl' (\l' n -> go l' n) l ch
          go l (Collision _ ch)   = foldl' (\l' (L k v _) -> (k, v) : l') l ch

-- Lookup element, also update LRU (TODO: No LRU update implemented)
{-# INLINEABLE lookup #-}
lookup :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
lookup k' m = (m, go (hash k') k' 0 $ mHAMT m)
    where go !_ !_ !_ Empty = Nothing
          go h k _ (Leaf lh (L lk lv lt))
              | lh /= h   = Nothing
              | lk /= k   = Nothing
              | otherwise = Just lv
          go h k s (Node _ ch) = go h k (s + bitsPerSubkey) (ch `V.unsafeIndex` indexNode h s)
          go h k _ (Collision colh ch)
              | colh == h = (\(L _ lv _) -> lv) <$> find (\(L lk _ _) -> lk == k) ch
              | otherwise = Nothing

-- Lookup element
{-# INLINEABLE lookupNoLRU #-}
lookupNoLRU :: (Eq k, Hashable k) => k -> Map k v -> Maybe v
lookupNoLRU k' m = go (hash k') k' 0 $ mHAMT m
    where go !_ !_ !_ Empty = Nothing
          go h k _ (Leaf lh (L lk lv lt))
              | lh /= h   = Nothing
              | lk /= k   = Nothing
              | otherwise = Just lv
          go h k s (Node _ ch) = go h k (s + bitsPerSubkey) (ch `V.unsafeIndex` indexNode h s)
          go h k _ (Collision colh ch)
              | colh == h = (\(L _ lv _) -> lv) <$> find (\(L lk _ _) -> lk == k) ch
              | otherwise = Nothing

-- TODO: No LRU update implemented
{-# INLINEABLE delete #-}
delete :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
delete k' m =
    let go !_ !_ !_ Empty = (Empty, Nothing)
        go h k _ t@(Leaf lh (L lk lv lt))
            | lh /= h   = (t, Nothing)
            | lk /= k   = (t, Nothing)
            | otherwise = (Empty, Just lv)
        go h k s t@(Node _ ch) =
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
                              _             -> (Node (OldNew 0 0) ch', del')
                _      -> (Node (OldNew 0 0) ch', del')
        go h k _ t@(Collision colh ch)
            | colh == h = let (delch', ch') = partition (\(L lk _ _) -> lk == k) ch
                          in  if   length ch' == 1
                              then  -- Deleted last remaining collision, it's a leaf node now
                                   (Leaf h $ head ch', Just $ (\((L _ lv _):[]) -> lv) delch')
                              else (Collision h ch', (\(L _ lv _) -> lv) <$> listToMaybe delch')
            | otherwise = (t, Nothing)
        !(m', del) = go (hash k') k' 0 $ mHAMT m
    in  ( m { mHAMT = m'
            , mSize = mSize m - if isJust del then 1 else 0
            }
        , del
        )

popNewest, popOldest :: (Eq k, Hashable k) => Map k v -> (Map k v, Maybe (k, v))
popNewest = popInternal False
popOldest = popInternal True

-- Delete and return most / least recently used item
--
-- TODO: We first find the item and then delete it by key, could do this with a
--       single traversal instead
popInternal :: (Eq k, Hashable k) => Bool -> Map k v -> (Map k v, Maybe (k, v))
popInternal popOld m =
    case go $ mHAMT m of
        Just k  -> let (m', Just v) = delete k m in (m', Just (k, v))
        Nothing -> (m, Nothing)
    where go Empty                      = Nothing
          go (Leaf _ (L lk _ _))        = Just lk
          go (Node (OldNew old new) ch) = go $ ch `V.unsafeIndex` if popOld then old else new
          go (Collision _ ch)           = Just . (\(L lk _ _) -> lk)
                                               . ( if   popOld
                                                   then minimumBy
                                                   else maximumBy
                                                 )
                                                 (\(L _ _ a) (L _ _ b) -> compare a b)
                                                 $ ch

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
                       Leaf h (L k v _) -> checkKey h k v
                       Collision h ch -> do
                           when (length ch < 2) $
                               tell "Hash collision node with <2 children\n"
                           forM_ ch $ \(L lk lv lt) -> checkKey h lk lv
                       Node _ ch -> do
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

