
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
import Data.Word
import Data.List (find, partition)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import Control.Applicative hiding (empty)
import Control.Monad
import Control.Monad.Writer

-- Associative array implemented on top of a Hashed Array Mapped Trie (HAMT), based on the
-- implementation in Data.HashMap. Basically a prefix tree over the bits of key hashes, with a
-- higher than binary branching factor. Additional least / most recently used bounds for
-- subtrees are stored so the data structure can have an upper bound on the number of elements
-- and remove the least recently used one overflow. The other bound allows to retrieve the item
-- which was inserted / touched last

data Map k v = Map { mLimit :: !Int
                   , mTick  :: !Word64 -- We use a 'tick', which we keep incrementing, to keep
                                       -- track of how old elements are relative to each other
                   , mHAMT  :: !(HAMT k v)
                   }

type Hash = Word

hash :: H.Hashable a => a -> Hash
hash = fromIntegral . H.hash

data Leaf k v = L !k !v

-- Note that we don't have bitmap indexing for partially filled nodes. This simplifies and
-- speeds up the code, but comes at the expensive of memory usage and access
data HAMT k v = Empty
              | Leaf !Hash !(Leaf k v)
              | Node !(V.Vector (HAMT k v))
              | Collision !Hash ![Leaf k v]

bitsPerSubkey :: Int
bitsPerSubkey = 4

subkeyMask :: Hash
subkeyMask = (1 `shiftL` bitsPerSubkey) - 1

-- Retrieve a leaf child index from a hash and a subkey offset
indexNode :: Hash -> Int -> Int
indexNode h s = fromIntegral $ (h `shiftR` s) .&. subkeyMask

maxChildren :: Int
maxChildren = 1 `shiftL` bitsPerSubkey

-- Insert a new element into the map, return the new map and the truncated
-- element (if over the limit)
{-# INLINEABLE insert #-}
insert :: (Eq k, Hashable k) => k -> v -> Map k v -> (Map k v, Maybe (k, v))
insert k v m = insertInternal False k v m

{-# INLINE insertInternal #-}
insertInternal :: (Eq k, Hashable k) => Bool -> k -> v -> Map k v -> (Map k v, Maybe (k, v))
insertInternal updateOnly kIns vIns m =
    let go h k v _ Empty = Leaf h (L k v)
        go h k v s t@(Leaf lh li@(L lk lv)) =
            if   h == lh
            then if   k == lk
                 then Leaf h (L k v) -- Update value
                 else Collision h [L k v, li] -- We have a hash collision
            else -- Expand leaf into interior node
                 Node $ V.create $ do
                     vec <- VM.replicate maxChildren Empty
                     let ia = indexNode h  s
                         ib = indexNode lh s
                      in if   ia /= ib -- Subkey collision?
                         then do VM.write vec ia $ Leaf h (L k v)
                                 VM.write vec ib t
                         else -- Collision, add one level
                              VM.write vec ia $ go h k v (s + bitsPerSubkey) t
                     return vec
        go h k v s t@(Node ch) =
            let idx      = indexNode h s
                subtree  = ch V.! idx 
                subtree' = -- Traverse into child with matching subkey
                           go h k v (s + bitsPerSubkey) subtree
            in  Node $ ch V.// [(idx, subtree')]
        go h k v s t@(Collision colh ch) =
            if   h == colh
            then let traverse [] = [L k v] -- Append new leaf
                     traverse (l@(L lk lv):xs) =
                          if   lk == k
                          then L k v : xs -- Update value
                          else l : traverse xs
                 in  Collision h $ traverse ch
            else -- Expand collision into interior node
                 go h k v s . Node $ V.create $ do
                     vec <- VM.replicate maxChildren Empty
                     VM.write vec (indexNode colh s) t
                     return vec
    in  ( m { mHAMT = go (hash kIns) kIns vIns 0 $ mHAMT m
            }
        , Nothing
        )

empty :: Int -> Map k v
empty limit | limit >= 1 = Map { mLimit = limit
                               , mTick  = 0
                               , mHAMT  = Empty
                               }
            | otherwise  = error "limit for LRUBoundedMap needs to be >= 1"

{-# INLINEABLE size #-}
size :: Map k v -> (Int, Int)
size m = (go 0 $ mHAMT m, mLimit m)
    where go n Empty = n
          go n (Leaf _ _) = n + 1
          go n (Node ch) = V.foldl' (go) n ch
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
toList m = undefined

-- Lookup element, also update LRU
{-# INLINEABLE lookup #-}
lookup :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
lookup k' m = (go (hash k') k' 0 $ mHAMT m) `seq` (m, go (hash k') k' 0 $ mHAMT m)
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
    let go !_ !_ !_ Empty = Empty
        go h k _ t@(Leaf lh (L lk _))
            | lh /= h   = t 
            | lk /= k   = t
            | otherwise = Empty
        go h k s t@(Node ch) =
            let idx      = indexNode h s
                subtree  = ch `V.unsafeIndex` idx 
                subtree' = go h k (s + bitsPerSubkey) subtree
                ch'      = ch V.// [(idx, subtree')]
                used     = -- Indices of used slots in the child vector
                           -- TODO: Would use fold', but there's a compiler bug:
                           --       https://ghc.haskell.org/trac/ghc/ticket/8547
                           V.foldr (\t' u -> u `seq` case t' of Empty -> u; _ -> t' : u) [] ch'
            in  case used of
                    []     -> Empty
                    (x:[]) -> -- If the last remaining child is a leaf / collision we can
                              -- replace the node with it. Otherwise, we still need the node
                              -- as there is a subkey collision
                              case x of Node _ -> Node ch'
                                        _      -> x
                    _      -> Node ch'
        go h k _ t@(Collision colh ch)
            | colh == h = let (delch', ch') = partition (\(L lk _) -> lk == k) ch
                          in  if   length ch' == 1
                              then  -- Deleted last remaining collision, it's a leaf node now
                                   Leaf h $ head ch'
                              else Collision h ch'
            | otherwise = t
    in  ( m { mHAMT = go (hash k') k' 0 $ mHAMT m
            }
        , Nothing
        )

-- Delete and return most recently used item
popNewest :: (Eq k, Hashable k) => Map k v -> (Map k v, Maybe (k, v))
popNewest m = undefined

-- Delete and return least recently used item
popOldest :: (Eq k, Hashable k) => Map k v -> (Map k v, Maybe (k, v))
popOldest m = undefined

{-# INLINEABLE update #-}
update :: (Eq k, Hashable k) => k -> v -> Map k v -> Map k v
update k v m = undefined

valid :: (Eq k, Hashable k) => Map k v -> Maybe String
valid m =
    let w = execWriter $ do
                when (mLimit m < 1) $ tell "limit < 1\n"
                --when ((fst $ size m) > mLimit m) $ tell "Size over the limit\n"
    in  case w of [] -> Nothing
                  xs -> Just xs

