
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
import Data.Int
import Data.List hiding (lookup, delete, null, insert)
import qualified Data.List (null)
import Control.Applicative hiding (empty)
import Control.Monad
import Control.Monad.Writer
import Control.DeepSeq (NFData(rnf))

-- Associative array implemented on top of a Hashed Trie. Basically a prefix tree over
-- the bits of key hashes. Additional least / most recently used bounds for subtrees are
-- stored so the data structure can have an upper bound on the number of elements and
-- remove the least recently used one overflow. The other bound allows to retrieve the
-- item which was inserted / touched last

type Tick = Word64 -- TODO: 64 bit integers are incredibly slow on 32bit GHC, huge speedup
                   --       when using a Word instead

data Map k v = Map { mLimit :: !Int
                   , mTick  :: !Tick -- We use a 'tick', which we keep incrementing, to keep
                                     -- track of how old elements are relative to each other
                   , mSize  :: !Int -- Cached to make size O(1) instead of O(n)
                   , mTrie  :: !(Trie k v)
                   }

instance (NFData k, NFData v) => NFData (Map k v) where
    rnf (Map l t s h) = rnf l `seq` rnf t `seq` rnf s `seq` rnf h

type Hash = Int

{-# INLINE hash #-}
hash :: H.Hashable a => a -> Hash
hash = fromIntegral . H.hash

data Leaf k v = L !k !v !Tick

instance (NFData k, NFData v) => NFData (Leaf k v) where
    rnf (L k v _) = rnf k `seq` rnf v

data Trie k v = Empty
                -- Oldest A / Newest A / Oldest B / Newest B / A / B
              | Node !Tick !Tick !Tick !Tick !(Trie k v) !(Trie k v)
              | Leaf !Hash !(Leaf k v)
              | Collision !Hash ![Leaf k v]

{-# INLINE minMaxFromTrie #-}
minMaxFromTrie :: Trie k v -> (Tick, Tick)
minMaxFromTrie Empty                      = (maxBound, minBound)
minMaxFromTrie (Node olda newa oldb newb _ _) = (min olda oldb, max newa newb)
minMaxFromTrie (Leaf _ (L _ _ tick))      = (tick, tick)
minMaxFromTrie (Collision _ ch)           = ( (minimum . map (\(L _ _ tick) -> tick) $ ch)
                                            , (maximum . map (\(L _ _ tick) -> tick) $ ch)
                                            )

instance (NFData k, NFData v) => NFData (Trie k v) where
    rnf Empty              = ()
    rnf (Leaf _ l)         = rnf l
    rnf (Node _ _ _ _ a b) = rnf a `seq` rnf b
    rnf (Collision _ ch)   = rnf ch

{-# INLINE isA #-}
{-# INLINE isB #-}
isA, isB :: Hash -> Int -> Bool
isA h s = h .&. (1 `shiftL` s) == 0
isB h s = not $ isA h s

-- Are two hashes colliding at the given depth?
{-# INLINE subkeyCollision #-}
subkeyCollision :: Hash -> Hash -> Int -> Bool
subkeyCollision a b s = (a `xor` b) .&. (1 `shiftL` s) == 0

-- Insert a new element into the map, return the new map and the truncated
-- element (if over the limit)
{-# INLINEABLE insert #-}
insert :: (Eq k, Hashable k) => k -> v -> Map k v -> (Map k v, Maybe (k, v))
insert !k !v !m = insertInternal False k v m

{-# INLINEABLE update #-}
update :: (Eq k, Hashable k) => k -> v -> Map k v -> Map k v
update k v m =
    case insertInternal True k v m of
        (m', Nothing) -> m'
        _             -> error "LRUBoundedMap.update: insertInternal truncated with updateOnly"

{-# INLINE insertInternal #-}
insertInternal :: (Eq k, Hashable k) => Bool -> k -> v -> Map k v -> (Map k v, Maybe (k, v))
insertInternal !updateOnly {- TODO: captured -} !kIns !vIns !m =
    let go !h !k !v !_ Empty = if   updateOnly
                               then (Empty, (maxBound, minBound), False) -- Update mode, no insert
                               else (Leaf h $ L k v tick, (tick, tick), True)
        go !h !k !v !s !t@(Leaf !lh !li@(L !lk !lv !lt)) =
            let mint = min tick lt
                maxt = max tick lt
            in  if   h == lh
                then if   k == lk
                     then (Leaf h $ L k v tick, (tick, tick), False) -- Update value
                     else -- We have a hash collision, change to a collision node and insert
                          if   updateOnly -- ...unless we're in update mode
                          then (t, (lt, lt), False)
                          else (Collision h [L k v tick, li], (mint, maxt), True)
                else -- Expand leaf into interior node
                     if   updateOnly
                     then (t, (lt, lt), False)
                     else let t' = Leaf h (L k v tick)
                              (a', b') | -- Subkey collision, add one level
                                         subkeyCollision h lh s = (if   isA h s
                                                                  then flip (,) Empty
                                                                  else      (,) Empty)
                                                                      . (\(x, _, True) -> x)
                                                                      $ go h k v (s + 1) t
                                       | otherwise              = ( if isA h  s then t' else t
                                                                  , if isB lh s then t  else t'
                                                                  )
                          in ( Node (fst $ minMaxFromTrie a')
                                    (snd $ minMaxFromTrie a')
                                    (fst $ minMaxFromTrie b')
                                    (snd $ minMaxFromTrie b')
                                    a'
                                    b'
                             , (mint, maxt)
                             , True
                             )
        go !h !k !v !s !t@(Node _ _ _ _ a b) =
            let !((tA, (mintA, maxtA), insA), (tB, (mintB, maxtB), insB)) =
                    -- Traverse into child with matching subkey
                    if   isA h s
                    then (go h k v (s + 1) a, (b, minMaxFromTrie b, False))
                    else ((a, minMaxFromTrie a, False), go h k v (s + 1) b)
                mint = min mintA mintB
                maxt = max maxtA maxtB
            in  ( Node mintA maxtA mintB maxtB tA tB
                , (mint, maxt)
                , insA || insB
                )
        go !h !k !v !s !t@(Collision colh ch) =
            if   updateOnly
            then if   h == colh
                 then let traverseUO [] = [] -- No append in update mode
                          traverseUO (l@(L lk lv _):xs) =
                               if   lk == k
                               then L k v tick : xs
                               else l : traverseUO xs
                          t' = Collision h $! traverseUO ch
                      in (t', minMaxFromTrie t', False)
                 else (t, minMaxFromTrie t, False)
            else if   h == colh
                 then let traverse [] = [L k v tick] -- Append new leaf
                          traverse (l@(L lk lv _):xs) =
                              if   lk == k
                              then L k v tick : xs -- Update value
                              else l : traverse xs
                          t' = Collision h $! traverse ch
                      in (t', minMaxFromTrie t', length ch /= length (traverse ch)) -- TODO: Slow
                 else -- Expand collision into interior node
                      go h k v s $ Node maxBound minBound maxBound minBound
                          (if isA colh s then t else Empty)
                          (if isB colh s then t else Empty)
        !(trie', _, didInsert) = go (hash kIns) kIns vIns 0 $ mTrie m
        !tick = mTick m
        !inserted = m { mTrie = trie'
                      , mSize = mSize m + if didInsert then 1 else 0
                      , mTick = tick + 1
                      }
    in  inserted `seq` mSize m `seq`
        -- Overflow?
        if   mSize inserted > mLimit inserted
        then popOldest inserted
        else (inserted , Nothing)

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
          go (Node _ _ _ _ a b) = go a + go b
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
          go l (Node _ _ _ _ a b)     = go (go l a) b
          go l (Collision _ ch)   = foldl' (\l' (L k v _) -> (k, v) : l') l ch

-- Lookup element, also update LRU
{-# INLINEABLE lookup #-}
lookup :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
lookup k' m = ( m { mTick = tick + 1
                  , mTrie = trie'
                  }
              , mkey
              )
    where go !_ !_ !_ Empty = (Nothing, Empty)
          go h k _ t@(Leaf lh (L lk lv lt))
              | lh /= h   = (Nothing, t)
              | lk /= k   = (Nothing, t)
              | otherwise = (Just lv, Leaf lh (L lk lv tick))
          go h k s (Node mina maxa minb maxb a b) =
              -- Traverse into child with matching subkey
              let !(!ins, !t')      = go h k (s + 1) (if isA h s then a else b)
                  !(!mint', !maxt') = minMaxFromTrie t'
              in  if   isA h s
                  then (ins, Node mint' maxt' minb  maxb  t' b )
                  else (ins, Node mina  maxa  mint' maxt' a  t')
          go h k _ t@(Collision colh ch)
              | colh == h = -- Search child list for matching key, rebuild with updated tick
                            foldl' (\(r, Collision _ ch') l@(L lk lv _) ->
                                       if   lk == k
                                       then (Just lv, Collision colh $ (L lk lv tick) : ch')
                                       else (r      , Collision colh $ l              : ch')
                                   )
                                   (Nothing, Collision colh [])
                                   ch
              | otherwise = (Nothing, t)
          !tick = mTick m
          !(!mkey, !trie') = go (hash k') k' 0 $ mTrie m

{-# INLINEABLE lookupNoLRU #-}
lookupNoLRU :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
lookupNoLRU k' m = (m, go (hash k') k' 0 $ mTrie m)
    where go !_ !_ !_ Empty = Nothing
          go h k _ (Leaf lh (L lk lv lt))
              | lh /= h   = Nothing
              | lk /= k   = Nothing
              | otherwise = Just lv
          go h k s (Node _ _ _ _ a b) = go h k (s + 1) (if isA h s then a else b)
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
        go h k s t@(Node _ _ _ _ a b) =
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
                      (a', b')                              -> let (minA, maxA) = minMaxFromTrie a'
                                                                   (minB, maxB) = minMaxFromTrie b'
                                                               in  Node minA maxA minB maxB a' b'
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
    where go Empty               = Nothing
          go (Leaf _ (L lk _ _)) = Just lk
          go (Node _ _ _ _ a b)      = go $ if   popOld
                                        then if minA < minB then a else b
                                        else if maxA > maxB then a else b
                                        where (minA, maxA) = minMaxFromTrie a
                                              (minB, maxB) = minMaxFromTrie b
          go (Collision _ ch)    = Just . (\(L lk _ _) -> lk)
                                        . ( if   popOld
                                            then minimumBy
                                            else maximumBy
                                          )
                                          (\(L _ _ a) (L _ _ b) -> compare a b)
                                          $ ch

-- Run a series of consistency checks on the structure inside of the map, return a list of
-- errors if any issues where encountered
valid :: (Eq k, Hashable k, Eq v) => Map k v -> Maybe String
valid m =
    let w =
         execWriter $ do
             when (mLimit m < 1) $
                 tell "Invalid limit (< 1)\n"
             when ((fst $ size m) /= sizeTraverse m) $
                 tell "Mismatch beween cached and actual size\n"
             when ((fst $ size m) > mLimit m)
                 $ tell "Size over the limit\n"
             allTicks <-
               let traverse s minParent maxParent ticks t =
                     case t of
                         Leaf h (L lk lv lt) ->
                             (: ticks) <$> checkKey h lk lv lt minParent maxParent
                         Collision h ch -> do
                             -- tell "Found collision\n"
                             when (length ch < 2) $
                                 tell "Hash collision node with <2 children\n"
                             foldM (\xs (L lk lv lt) ->
                                       (: xs) <$> checkKey h lk lv lt minParent maxParent
                                   )
                                   ticks
                                   ch
                         Node minA maxA minB maxB a b -> do
                             let mint = min minA minB
                                 maxt = max maxA maxB
                             when (s + 1 > bitSize (undefined :: Word)) $
                                 tell "Subkey shift too large during traversal\n"
                             when (mint < minParent || maxt > maxParent) $
                                 tell "Node min/max tick outside of parent interval\n"
                             let used = foldl' (\u x@(t', _, _) ->
                                                   case t' of Empty -> u; _ -> x : u
                                               )
                                        []
                                        $ (a, minA, maxA) : (b, minB, maxB) : []
                             when (length used == 0) $
                                 tell "Node with only empty children\n"
                             when (length used == 1) $
                                case (\((x, _, _) : _) -> x) used of
                                    Leaf      _ _ -> tell "Node with single Leaf child\n"
                                    Collision _ _ -> tell "Node with single Collision child\n"
                                    _             -> return ()
                             foldM (\xs (c, mint', maxt') ->
                                       traverse (s + 1) mint' maxt' xs c
                                   )
                                   ticks
                                   used
                         Empty -> return ticks
                   checkKey h k v tick minParent maxParent = do
                       when (hash k /= h) $
                           tell "Hash / key mismatch\n"
                       when (tick >= mTick m) $
                           tell "Tick of leaf matches / exceeds current tick\n"
                       when (tick < minParent || tick > maxParent) $
                           tell "Leaf min/max tick outside of parent interval\n"
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
                       return tick
               in  traverse 0 minBound maxBound [] $ mTrie m
             when (length allTicks /= mSize m) $
                 tell "Collection of all tick values used resulted in different size that mSize\n"
             unless (Data.List.null . filter (\x -> length x /= 1) . group . sort $ allTicks) $
                 tell "Duplicate tick value found\n"
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

