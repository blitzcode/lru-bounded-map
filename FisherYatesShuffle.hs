
module FisherYatesShuffle (shuffle) where

import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import Control.Monad
import System.Random

-- http://en.wikipedia.org/wiki/Fisher-Yates_shuffle
-- 
-- To initialize an array a of n elements to a randomly shuffled copy of source, both 0-based:
--   a[0] ← source[0]
--   for i from 1 to n − 1 do
--       j ← random integer with 0 ≤ j ≤ i
--       if j ≠ i
--           a[i] ← a[j]
--       a[j] ← source[i]

shuffle :: RandomGen g => g -> [a] -> [a]
shuffle g xs =
    V.toList $ V.create $ do
        vec <- VM.new $ length xs
        void $ foldM ( \g' (i, src) -> do
                           let (j, g'') = randomR (0, i) g'
                           unless (i == j) . VM.write vec i =<< vec `VM.read` j
                           VM.write vec j src
                           return g''
                     ) g (zip [0..] xs)
        return vec

