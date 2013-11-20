
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
import Data.Hashable
import Data.Bits
import Data.Word
import qualified Data.Vector as V
import Control.Monad
import Control.Monad.Writer

-- Associative array implemented on top of a Hashed Array Mapped Trie.
-- Basically a prefix tree over the bits of key hashes, with a higher than
-- binary branching factor. Additional least / most recently used bounds for
-- subtrees are stored so the data structure can have an upper bound on the
-- number of elements and remove the least recently used on overflow. The other
-- bound allows to retrieve the item which was inserted / touched last

data Map k v = Map { mLimit :: !Int
                   , mTick  :: !Word64
                   , mHAMT  :: !(HAMT k v)
                   }

data Leaf k v = L !k v

data HAMT k v = Empty
              | Leaf !Hash !(Leaf k v)
              | Node !(V.Vector (HAMT k v))
              | Collision !Hash ![Leaf k v]

type Hash = Int

empty :: Int -> Map k v
empty limit | limit >= 1 = Map { mLimit = limit
                               , mTick  = 0
                               , mHAMT  = Empty
                               }
            | otherwise  = error "limit for LRUBoundedMap needs to be >= 1"

size :: Map k v -> (Int, Int)
size m = (undefined, mLimit m)

null :: Map k v -> Bool
null m = undefined

member :: (Eq k, Hashable k) => k -> Map k v -> Bool
member k = undefined

notMember :: (Eq k, Hashable k) => k -> Map k v -> Bool
notMember k m = not $ member k m

toList :: Map k v -> [(k, v)]
toList m = undefined

-- Lookup element, also update LRU
lookup :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
lookup k m = undefined

delete :: (Eq k, Hashable k) => k -> Map k v -> (Map k v, Maybe v)
delete k m = undefined

-- Delete and return most recently used item
popNewest :: (Eq k, Hashable k) => Map k v -> (Map k v, Maybe (k, v))
popNewest m = undefined

-- Delete and return least recently used item
popOldest :: (Eq k, Hashable k) => Map k v -> (Map k v, Maybe (k, v))
popOldest m = undefined

update :: (Eq k, Hashable k) => k -> v -> Map k v -> Map k v
update k v m = undefined

-- Insert a new element into the map, return the new map and the truncated
-- element (if over the limit)
insert :: (Eq k, Hashable k) => k -> v -> Map k v -> (Map k v, Maybe (k, v))
insert = undefined

valid :: (Eq k, Hashable k) => Map k v -> Maybe String
valid m =
    let w = execWriter $ do
                when (mLimit m < 1) $ tell "limit < 1\n"
                when ((fst $ size m) > mLimit m) $ tell "Size over the limit\n"
    in  case w of [] -> Nothing
                  xs -> Just xs

