
{-# LANGUAGE BangPatterns #-}
{-# OPTIONS_GHC -fno-full-laziness -funbox-strict-fields #-}

module LRUBoundedMap_CustomHashedTrie ( Map
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
import Data.Foldable (minimumBy, maximumBy)
import Data.List (find, partition, foldl')
import Control.Applicative hiding (empty)
import Control.Monad
import Control.Monad.Writer
import Control.DeepSeq (NFData(rnf))

-- Associative array implemented on top of a Hashed Trie. Basically a prefix tree over
-- the bits of key hashes. Additional least / most recently used bounds for subtrees are
-- stored so the data structure can have an upper bound on the number of elements and
-- remove the least recently used one overflow. The other bound allows to retrieve the
-- item which was inserted / touched last

data Map k v = Map { mLimit :: !Int
                   , mTick  :: !Word64 -- We use a 'tick', which we keep incrementing, to keep
                                       -- track of how old elements are relative to each other
                   , mSize  :: !Int -- Cached to make size O(1) instead of O(n)
                   , mTrie  :: !(Trie k v)
                   }

instance (NFData k, NFData v) => NFData (Map k v) where
    rnf (Map l t s h) = rnf l `seq` rnf t `seq` rnf s `seq` rnf h

type Hash = Word

{-# INLINE hash #-}
hash :: H.Hashable a => a -> Hash
hash = fromIntegral . H.hash

data Leaf k v = L !k !v !Word64 -- LRU tick

instance (NFData k, NFData v) => NFData (Leaf k v) where
    rnf (L k v t) = rnf k `seq` rnf v `seq` rnf t

data OldNew = OldNew !Int !Int

data Trie k v = Empty
              | Node !OldNew !(Trie k v) !(Trie k v)
              | Leaf !Hash !(Leaf k v)
              | Collision !Hash ![Leaf k v]

instance (NFData k, NFData v) => NFData (Trie k v) where
    rnf Empty            = ()
    rnf (Leaf _ l)       = rnf l
    rnf (Node m a b)     = m `seq` rnf a `seq` rnf b
    rnf (Collision _ ch) = rnf ch

{-# INLINE isA #-}
{-# INLINE isB #-}
isA, isB :: Hash -> Int -> Bool
isA h s = h .&. (1 `shiftL` s) == 0
isB h s = not $ isA h s

-- Insert a new element into the map, return the new map and the truncated
-- element (if over the limit)
{-# INLINEABLE insert #-}
insert :: (Eq k, Hashable k) => k -> v -> Map k v -> (Map k v, Maybe (k, v))
insert !k !v !m = insertInternal False k v m

data Pair a b = Pair !a !b

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
                 else let !ia = isA h  s
                          !ib = isA lh s
                          subkeyCol = ia == ib
                          ins = Leaf h (L k v tick)
                          (a', b') | -- Subkey collision, add one level
                                     subkeyCol = (\(Pair x _) -> if   ia
                                                                 then (x, Empty)
                                                                 else (Empty, x)
                                                 )
                                                 $ go h k v (s + 1) t
                                   | otherwise = ( if ia then ins else t
                                                 , if ib then ins else t
                                                 )
                      in Pair (Node (OldNew 0 0) a' b') True
        go !h !k !v !s !t@(Node _ a b) =
            let !(a', b', i) = -- Traverse into child with matching subkey
                               if   isA h s
                               then (\(Pair t' ichild) -> (t', b , ichild)) $ go h k v (s + 1) a
                               else (\(Pair t' ichild) -> (a , t', ichild)) $ go h k v (s + 1) b
            in  Pair (Node (OldNew 0 0) a' b') i
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
                      go h k v s $ Node (OldNew 0 0)
                          (if isA colh s then t else Empty)
                          (if isB colh s then t else Empty)
        !(Pair m' i') = go (hash kIns) kIns vIns 0 $ mTrie m
        !tick = mTick m
    in  m' `seq` i' `seq` mSize m `seq`
        ( m { mTrie = m'
            , mSize = mSize m + if i' then 1 else 0
            , mTick = tick + 1
            }
        , Nothing
        )

empty :: Int -> Map k v
empty limit | limit >= 1 = Map { mLimit = limit
                               , mTick  = 0
                               , mSize  = 0
                               , mTrie  = Empty
                               }
            | otherwise  = error "limit for LRUBoundedMap needs to be >= 1"

{-# INLINEABLE size #-}
size :: Map k v -> (Int, Int)
size m = (mSize m, mLimit m)

-- O(n) size-by-traversal
sizeTraverse :: Map k v -> Int
sizeTraverse m = go $ mTrie m
    where go Empty            = 0
          go (Leaf _ _)       = 1
          go (Node _ a b)     = go a + go b
          go (Collision _ ch) = length ch

{-# INLINEABLE null #-}
null :: Map k v -> Bool
null m = case mTrie m of Empty -> True; _ -> False

{-# INLINEABLE member #-}
member :: (Eq k, Hashable k) => k -> Map k v -> Bool
member k m = isJust . snd $ lookup k m

{-# INLINEABLE notMember #-}
notMember :: (Eq k, Hashable k) => k -> Map k v -> Bool
notMember k m = not $ member k m

{-# INLINEABLE toList #-}
toList :: Map k v -> [(k, v)]
toList m = go [] $ mTrie m
    where go l Empty              = l
          go l (Leaf _ (L k v _)) = (k, v) : l
          go l (Node _ a b)       = go (go l a) b
          go l (Collision _ ch)   = foldl' (\l' (L k v _) -> (k, v) : l') l ch

-- Lookup element, also update LRU
{-# INLINEABLE lookup #-}
lookup :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
lookup k' m = (m, go (hash k') k' 0 $ mTrie m)
    where go !_ !_ !_ Empty = Nothing
          go h k _ (Leaf lh (L lk lv lt))
              | lh /= h   = Nothing
              | lk /= k   = Nothing
              | otherwise = Just lv
          go h k s (Node _ a b) = go h k (s + 1) (if isA h s then a else b)
          go h k _ (Collision colh ch)
              | colh == h = (\(L _ lv _) -> lv) <$> find (\(L lk _ _) -> lk == k) ch
              | otherwise = Nothing

{-# INLINEABLE delete #-}
delete :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
delete k' m =
    let go !_ !_ !_ Empty = (Empty, Nothing)
        go h k _ t@(Leaf lh (L lk lv lt))
            | lh /= h   = (t, Nothing)
            | lk /= k   = (t, Nothing)
            | otherwise = (Empty, Just lv)
        go h k s t@(Node _ a b) =
            let !(ch, del') = if   isA h s
                              then (\(t', dchild) -> ((t', b ), dchild)) $ go h k (s + 1) a
                              else (\(t', dchild) -> ((a , t'), dchild)) $ go h k (s + 1) b
            in  ( case ch of
                      -- We removed the last element, delete node
                      (Empty, Empty)                        -> Empty
                      -- If our last child is a leaf / collision replace the node by it
                      (Empty, t'   ) | isLeafOrCollision t' -> t'
                      (t'   , Empty) | isLeafOrCollision t' -> t'
                      -- Update node with new subtree
                      (a', b')                              -> Node (OldNew 0 0) a' b'
                , del'
                )
        go h k _ t@(Collision colh ch)
            | colh == h = let (delch', ch') = partition (\(L lk _ _) -> lk == k) ch
                          in  if   length ch' == 1
                              then  -- Deleted last remaining collision, it's a leaf node now
                                   (Leaf h $ head ch', Just $ (\((L _ lv _):[]) -> lv) delch')
                              else (Collision h ch', (\(L _ lv _) -> lv) <$> listToMaybe delch')
            | otherwise = (t, Nothing)
        !(m', del) = go (hash k') k' 0 $ mTrie m
        isLeafOrCollision (Leaf _ _)      = True
        isLeafOrCollision (Collision _ _) = True
        isLeafOrCollision _               = False
    in  ( m { mTrie = m'
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
    case go $ mTrie m of
        Just k  -> let (m', Just v) = delete k m in (m', Just (k, v))
        Nothing -> (m, Nothing)
    where go Empty                       = Nothing
          go (Leaf _ (L lk _ _))         = Just lk
          --go (Node (OldNew old new) _ _) = go $ ch `V.unsafeIndex` if popOld then old else new
          go (Collision _ ch)            = Just . (\(L lk _ _) -> lk)
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
                           forM_ ch $ \(L lk lv _) -> checkKey h lk lv
                       Node _ a b -> do
                           when (s + 1 > bitSize (undefined :: Word)) $
                               tell "Subkey shift too large during traversal\n"
                           let used = foldl' (\u t' -> case t' of Empty -> u; _ -> t' : u)
                                      []
                                      $ a : b : []
                           when (length used == 0) $
                               tell "Node with only empty children\n"
                           when (length used == 1) $
                              case head used of
                                  Leaf      _ _ -> tell "Node with single Leaf child\n"
                                  Collision _ _ -> tell "Node with single Collision child\n"
                                  _             -> return ()
                           forM_ used . traverse $ s + 1
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
              in traverse 0 $ mTrie m
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

