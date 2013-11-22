
module Main where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.List
import Data.Maybe
import Data.Hashable
import Text.Printf
import Control.Applicative
import Control.Monad
import Control.DeepSeq (NFData(rnf))
import Criterion.Main
import Criterion.Config
import System.Exit

import qualified Data.Map.Strict as M
import qualified Data.HashMap.Strict as HM
import qualified Data.IntMap.Strict as IM
import qualified LRUBoundedMap_LinkedListHashMap as LBM_LLHM
import qualified LRUBoundedMap_DoubleMapBTree as LBM_DMBT
import qualified LRUBoundedMap_CustomHAMT as LBM_CHAMT

criterionCfg :: Config
criterionCfg = defaultConfig { cfgPerformGC = ljust True
                             , cfgReport    = ljust "./report.html"
                             , cfgSamples   = ljust 10
                             }

main :: IO ()
main = do
    -- Load test set of representative keys
    keysL <- B8.lines <$> B.readFile "keys.txt"
    let numK  = printf "(%ik keys)" $ length keysL `div` 1000
        kvL   = zip keysL ([1..] :: [Int])
        -- Make initial maps for delete / lookup benchmarks
        mkDMS         = foldl' (\r (k, v) ->       M.insert k v r)         (M.empty)
        mkDHMS        = foldl' (\r (k, v) ->       HM.insert k v r)        (HM.empty)
        mkDIMS        = foldl' (\r (k, v) ->       IM.insert (hash k) v r) (IM.empty)
        mkLBM_LLHM5k  = foldl' (\r (k, v) -> fst $ LBM_LLHM.insert k v r)  (LBM_LLHM.empty  5000)
        mkLBM_LLHM1k  = foldl' (\r (k, v) -> fst $ LBM_LLHM.insert k v r)  (LBM_LLHM.empty  1000)
        mkLBM_DMBT5k  = foldl' (\r (k, v) -> fst $ LBM_DMBT.insert k v r)  (LBM_DMBT.empty  5000)
        mkLBM_DMBT1k  = foldl' (\r (k, v) -> fst $ LBM_DMBT.insert k v r)  (LBM_DMBT.empty  1000)
        mkLBM_CHAMT5k = foldl' (\r (k, v) -> fst $ LBM_CHAMT.insert k v r) (LBM_CHAMT.empty 5000)
        mkLBM_CHAMT1k = foldl' (\r (k, v) -> fst $ LBM_CHAMT.insert k v r) (LBM_CHAMT.empty 1000)
    -- Some basic tests for the LRU HAMT map
    case LBM_CHAMT.valid $ mkLBM_CHAMT5k kvL of
        Just err -> (putStrLn $ "mkLBM_CHAMT5k.valid: " ++ err) >> exitFailure
        Nothing  -> return ()
    case LBM_CHAMT.valid $ mkLBM_CHAMT1k kvL of
        Just err -> (putStrLn $ "mkLBM_CHAMT1k.valid: " ++ err) >> exitFailure
        Nothing  -> return ()
    forM_ (LBM_LLHM.view $ mkLBM_LLHM5k kvL) $ \(k, v) ->
        case LBM_CHAMT.lookup k (mkLBM_CHAMT5k kvL) of
          (_, Just v') -> when (v /= v') $ do
                              putStrLn $ "mkLBM_CHAMT5k invalid value for key: " ++ B8.unpack k
                              exitFailure
          (_, Nothing) -> (putStrLn $ "mkLBM_CHAMT5k missing key: " ++ B8.unpack k) >> exitFailure
    when (LBM_LLHM.size (mkLBM_LLHM5k kvL) /= LBM_CHAMT.size (mkLBM_CHAMT5k kvL)) $
        (putStrLn "mkLBM_CHAMT5k size mismatch") >> exitFailure
    forM_ (LBM_LLHM.view $ mkLBM_LLHM1k kvL) $ \(k, v) ->
        case LBM_CHAMT.lookup k (mkLBM_CHAMT1k kvL) of
          (_, Just v') -> when (v /= v') $ do
                              putStrLn $ "mkLBM_CHAMT1k invalid value for key: " ++ B8.unpack k
                              exitFailure
          (_, Nothing) -> (putStrLn $ "mkLBM_CHAMT1k missing key: " ++ B8.unpack k) >> exitFailure
    {-
    when (LBM_LLHM.size (mkLBM_LLHM1k kvL) /= LBM_CHAMT.size (mkLBM_CHAMT1k kvL)) $
        (putStrLn "mkLBM_CHAMT1k size mismatch") >> exitFailure
    -}
    {-
    let (_, sizes) = foldl' (\(r, s) k -> let m = fst $ LBM_CHAMT.delete k r in (m, (fst $ LBM_CHAMT.size m) : s)) (mkLBM_CHAMT5k kvL, []) (map (fst) . LBM_CHAMT.toList . mkLBM_CHAMT5k $ kvL)
    --print sizes
    forM_ (zip sizes $ [0..]) $ \(a, b) -> printf "%i / %i %s\n" a b $ if (a /= b) then "M" else ""
    -}
    let allDeleted = foldl' (\r k -> fst $ LBM_CHAMT.delete k r) (mkLBM_CHAMT5k kvL) keysL
     in do when ((fst $ LBM_CHAMT.size allDeleted) /= 0) $
              (putStrLn "mkLBM_CHAMT5k delete failed") >> exitFailure
           case LBM_CHAMT.valid allDeleted of
               Just err -> (putStrLn $ "delete: mkLBM_CHAMT5k.valid: " ++ err) >> exitFailure
               Nothing  -> return ()
    {-
    let lookups    = (map (fst) . take 100 . drop 4000 $ kvL) ++
                     (map (fst) . take 100             $ kvL)
        insertions = [(B8.pack $ show i, i) | i <- [1..50]]
        reference  = sort . LBM_LLHM.view $
                       foldl' (\r (k, v) -> fst $ LBM_LLHM.insert k v r)
                              ( foldl' (\r k -> fst $ LBM_LLHM.lookup k r)
                                       (mkLBM_LLHM1k kvL)
                                       lookups
                              )
                              insertions
        testMap    = foldl' (\r (k, v) -> fst $ LBM_CHAMT.insert k v r)
                            ( foldl' (\r k -> fst $ LBM_CHAMT.lookup k r)
                                     (mkLBM_CHAMT1k kvL)
                                     lookups
                            )
                            insertions
        test       = sort LBM_CHAMT.toList $ testMap
     in do case LBM_CHAMT.valid testMap of
               Just err -> do putStrLn $ "lookup / insert / delete test valid: " ++ err
                              exitFailure
               Nothing  -> return ()
           when (test /= reference) $
              (putStrLn "mkLBM_CHAMT1k lookup / insert / delete comparison failed") >> exitFailure
    -}
    -- Make sure we build the initial maps
    rnf (mkDMS           kvL) `seq`
      rnf (mkDHMS        kvL) `seq`
      rnf (mkDIMS        kvL) `seq`
      rnf (mkLBM_LLHM5k  kvL) `seq`
      rnf (mkLBM_LLHM1k  kvL) `seq`
      rnf (mkLBM_DMBT5k  kvL) `seq`
      rnf (mkLBM_DMBT1k  kvL) `seq`
      rnf (mkLBM_CHAMT5k kvL) `seq`
      rnf (mkLBM_CHAMT1k kvL) `seq`
      -- Run criterion benchmarks
      defaultMainWith
        criterionCfg
        (return ())
        [
          bgroup ("insert " ++ numK)
          [
            bench "Data.Map.Strict (no LRU&limit)"     . nf (mkDMS)         $ kvL
          , bench "Data.HashMap.Strict (no LRU&limit)" . nf (mkDHMS)        $ kvL
          , bench "Data.IntMap.Strict (no LRU&limit)"  . nf (mkDIMS)        $ kvL
          {- -- Disabled, too slow (uses O(n) size call)
          , bench "LBM_LinkedListHashMap (limit 5k)"   . nf (mkLBM_LLHM5k)  $ kvL
          , bench "LBM_LinkedListHashMap (limit 1k)"   . nf (mkLBM_LLHM1k)  $ kvL
          -}
          , bench "LBM_DoubleMapBTree (limit 5k)"      . nf (mkLBM_DMBT5k)  $ kvL
          , bench "LBM_DoubleMapBTree (limit 1k)"      . nf (mkLBM_DMBT1k)  $ kvL
          , bench "LBM_CustomHAMT (limit 5k)"          . nf (mkLBM_CHAMT5k) $ kvL
          , bench "LBM_CustomHAMT (limit 1k)"          . nf (mkLBM_CHAMT1k) $ kvL
          ]
        , bgroup ("delete " ++ numK)
          [
            bench "Data.Map.Strict (no LRU&limit)" . nf
              (foldl' (\r k -> M.delete k r) (mkDMS kvL)) $ keysL
          , bench "Data.HashMap.Strict (no LRU&limit)" . nf
              (foldl' (\r k -> HM.delete k r) (mkDHMS kvL)) $ keysL
          , bench "Data.IntMap.Strict (no LRU&limit)" . nf
              (foldl' (\r k -> IM.delete (hash k) r) (mkDIMS kvL)) $ keysL
          , bench "LBM_LinkedListHashMap (limit 5k)" . nf
              (foldl' (\r k -> fst $ LBM_LLHM.delete k r) (mkLBM_LLHM5k kvL)) $ keysL
          , bench "LBM_LinkedListHashMap (limit 1k)" . nf
              (foldl' (\r k -> fst $ LBM_LLHM.delete k r) (mkLBM_LLHM1k kvL)) $ keysL
          , bench "LBM_DoubleMapBTree (limit 5k)" . nf
              (foldl' (\r k -> LBM_DMBT.delete k r) (mkLBM_DMBT5k kvL)) $ keysL
          , bench "LBM_DoubleMapBTree (limit 1k)" . nf
              (foldl' (\r k -> LBM_DMBT.delete k r) (mkLBM_DMBT1k kvL)) $ keysL
          , bench "LBM_CustomHAMT (limit 5k)" . nf
              (foldl' (\r k -> fst $ LBM_CHAMT.delete k r) (mkLBM_CHAMT5k kvL)) $ keysL
          , bench "LBM_CustomHAMT (limit 1k)" . nf
              (foldl' (\r k -> fst $ LBM_CHAMT.delete k r) (mkLBM_CHAMT1k kvL)) $ keysL
          ]
        , bgroup ("lookup (w/ LRU update) " ++ numK)
          [
            bench "Data.Map.Strict (no LRU&limit)" . nf
              (foldl' (\r k -> (r +) . fromJust . M.lookup k $ mkDMS kvL) 0) $ keysL
          , bench "Data.HashMap.Strict (no LRU&limit)" . nf
              (foldl' (\r k -> (r +) . fromJust . HM.lookup k $ mkDHMS kvL) 0) $ keysL
          , bench "Data.IntMap.Strict (no LRU&limit)" . nf
              (foldl' (\r k -> (r +) . fromJust . IM.lookup (hash k) $ mkDIMS kvL) 0) $ keysL
          , bench "LBM_LinkedListHashMap (limit 5k)" . nf
              (foldl' (\(r, a) k ->
                  (\(r', a') ->
                    (r', a + fromMaybe 0 a')) $
                  LBM_LLHM.lookup k r)
                (mkLBM_LLHM5k kvL, 0)) $
                keysL
          , bench "LBM_LinkedListHashMap (limit 1k)" . nf
              (foldl' (\(r, a) k ->
                  (\(r', a') ->
                    (r', a + fromMaybe 0 a')) $
                  LBM_LLHM.lookup k r)
                (mkLBM_LLHM1k kvL, 0)) $
                keysL
          , bench "LBM_DoubleMapBTree (limit 5k)" . nf
              (foldl' (\(r, a) k ->
                  (\(r', a') ->
                    (r', a + fromMaybe 0 a')) $
                  LBM_DMBT.lookup k r)
                (mkLBM_DMBT5k kvL, 0)) $
                keysL
          , bench "LBM_DoubleMapBTree (limit 1k)" . nf
              (foldl' (\(r, a) k ->
                  (\(r', a') ->
                    (r', a + fromMaybe 0 a')) $
                  LBM_DMBT.lookup k r)
                (mkLBM_DMBT1k kvL, 0)) $
                keysL
          , bench "LBM_CustomHAMT (limit 5k)" . nf
              (foldl' (\(r, a) k ->
                  (\(r', a') ->
                    (r', a + fromMaybe 0 a')) $
                  LBM_CHAMT.lookup k r)
                (mkLBM_CHAMT5k kvL, 0)) $
                keysL
          , bench "LBM_CustomHAMT (limit 1k)" . nf
              (foldl' (\(r, a) k ->
                  (\(r', a') ->
                    (r', a + fromMaybe 0 a')) $
                  LBM_CHAMT.lookup k r)
                (mkLBM_CHAMT1k kvL, 0)) $
                keysL
          ]
        ]

