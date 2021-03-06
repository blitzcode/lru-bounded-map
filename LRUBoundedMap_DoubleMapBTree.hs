
module LRUBoundedMap_DoubleMapBTree ( Map
                                    , empty
                                    , insert
                                    , insertUnsafe
                                    , member
                                    , notMember
                                    , lookup
                                    , delete
                                    , deleteFindNewest
                                    , update
                                    , size
                                    , view
                                    , valid
                                    ) where

import Prelude hiding (lookup)
import Data.Word
import Control.Monad.Writer
import Control.DeepSeq (NFData(rnf))
import Text.Printf

import qualified DoubleMap as DM
import qualified Data.Map.Strict as M

-- Bounded map maintaining a separate map for access history to drop the least
-- recently used element once the specified element limit is reached

data Map k v = Map !(DM.Map k Word64 v)
                   !Word64 -- We use a 'tick', which we keep incrementing, to keep track of how
                           -- old elements are relative to each other
                   !Int
                   deriving (Show)

instance (NFData k, NFData v) => NFData (Map k v) where
    rnf (Map m t l) = rnf m `seq` rnf t `seq` rnf l

empty :: Int -> Map k v
empty limit | limit >= 1 = Map DM.empty 0 limit
            | otherwise  = error "limit for LRUBoundedMap needs to be >= 1"

-- Insert a new element into the map, return the new map and the truncated
-- element (if over the limit)
insertInternal :: Ord k
               => (k -> Word64 -> v -> DM.Map k Word64 v -> DM.Map k Word64 v) -- DM insert
               -> k
               -> v
               -> Map k v
               -> (Map k v, Maybe (k, v))
insertInternal fins k v (Map m tick limit) =
    let inserted         = fins k tick v m
        (truncE, truncM) = if   DM.size inserted > limit
                           then let (lruKB, (lruKA, lruV)) = M.findMin (snd $ DM.view inserted)
                                in  (Just (lruKA, lruV), DM.deleteAB lruKA lruKB inserted)
                           else (Nothing, inserted)
    in  (Map truncM (tick + 1) limit, truncE)
insert :: Ord k => k -> v -> Map k v -> (Map k v, Maybe (k, v)) 
insert = insertInternal DM.insert
insertUnsafe :: Ord k => k -> v -> Map k v -> (Map k v, Maybe (k, v)) 
insertUnsafe = insertInternal DM.insertUnsafe

member :: Ord k => k -> Map k v -> Bool
member k (Map m _ _) = DM.member (Left k) m

notMember :: Ord k => k -> Map k v -> Bool
notMember k (Map m _ _) = DM.notMember (Left k) m

-- Lookup element, also update LRU time
lookup :: Ord k => k -> Map k v -> (Map k v, Maybe v)
lookup k bm@(Map m tick limit) = case DM.lookup (Left k) m of
    Just (_, kb, v) -> (Map (DM.updateKeyB kb tick m) (tick + 1) limit, Just v)
    Nothing         -> (bm, Nothing)

delete :: Ord k => k -> Map k v -> Map k v
delete k (Map m tick limit) = Map (DM.delete (Left k) m) tick limit

-- Remove and return most recently used element
deleteFindNewest :: Ord k => Map k v -> (Map k v, Maybe (k, v))
deleteFindNewest (Map m tick limit) = let (delMap, delVal) = DM.deleteFindMaxB m
                                      in  ( Map delMap tick limit
                                          , (\(ka, _, v) -> (ka, v)) <$> delVal
                                          )

-- Update value, don't touch LRU time
update :: Ord k => k -> v -> Map k v -> Map k v
update k v (Map m tick limit) = Map (DM.update (Left k) v m) tick limit

size :: Map k v -> (Int, Int)
size (Map m _ limit) = (DM.size m, limit)

view :: Map ka v -> (M.Map ka (Word64, v), M.Map Word64 (ka, v))
view (Map m _ _) = DM.view m

valid :: (Show k, Ord k) => Map k v -> Maybe String
valid (Map m tick limit) =
    let w = execWriter $ do
                when (limit < 1) $ tell "limit < 1\n"
                let (_, mb) = DM.view m
                forM_ (M.toList mb) $ \(kb, _) ->
                   when (kb >= tick) . tell $ printf "invalid tick in B map (%i > %i)\n" kb tick
                case DM.valid m of
                    Just xs -> tell xs
                    Nothing -> return ()
    in  case w of [] -> Nothing
                  xs -> Just xs

