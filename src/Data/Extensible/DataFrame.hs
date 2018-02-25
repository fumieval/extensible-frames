{-# OPTIONS_GHC -fno-warn-simplifiable-class-constraints #-}
module Data.Extensible.DataFrame
  ( DataFrame(..)
  -- * Construction
  , readCSVFile
  -- * slice
  , slice
  -- * Access
  , (#-)
  , (#|)
  -- * Foldl
  , (|=)
  -- * Quantiles
  , getTDigest
  , quartiles
  , median
  -- * Statistics
  , mean
  -- * Reexported types
  , Csv.DecodeOptions(..)
  , Csv.defaultDecodeOptions
  , T.Text
  , B.ByteString
  , ISO8601
  ) where

import Control.Monad.Primitive
import Data.Extensible
import Data.Extensible.DataFrame.Time
import Data.Extensible.DataFrame.Types
import Data.List (intercalate)
import Data.Proxy
import Data.Typeable
import GHC.TypeLits
import qualified Control.Foldl as F
import qualified Data.ByteString as B
import qualified Data.ByteString.Short as SB
import qualified Data.Csv as Csv
import qualified Data.Csv.Incremental as I
import qualified Data.Extensible.Struct as S
import qualified Data.TDigest as TD
import qualified Data.Text as T
import qualified Data.Text.Short as ST
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as MG
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Mutable as MU
import qualified Data.Vector as V
import Data.Vector.Unboxed.Deriving
import Data.Thyme (UTCTime)
import System.IO

slice :: Forall (KeyValue KnownSymbol ColumnType) xs
  => DataFrame xs
  -> Int -- ^ offset (negative value means an offset from the end)
  -> Int -- ^ length
  -> DataFrame xs
slice df m n = DataFrame
  { dfContent = htabulateFor proxyColumns $ \p -> Field $ WrapCV
    $ G.unsafeSlice ofs len
    $ unwrapCV $ getField $ hlookup p $ dfContent df
  , dfLength = len
  }
  where
    ofs = m `mod` dfLength df
    len = max 0 $ min n $ dfLength df - ofs

(#-) :: Forall (KeyValue KnownSymbol ColumnType) xs
  => DataFrame xs
  -> Int -- ^ index
  -> Record xs
DataFrame df len #- i
  | i < 0 || i >= len = error $ "rowAt: out of bounds (" ++ show len ++ ")"
  | otherwise = htabulateFor (Proxy :: Proxy (KeyValue KnownSymbol ColumnType))
    $ \j -> Field $ pure $ flip G.unsafeIndex i $ unwrapCV $ getField $ hlookup j df

infixl 4 #-

(#|) :: Associate k v xs
  => DataFrame xs
  -> Proxy k -- ^ column
  -> Column (k ':> v)
DataFrame df _ #| _ = hlookup association df

infixl 4 #|

-- | Apply a 'F.Fold' to a column.
(|=) :: ColumnType a => Column (k ':> a) -> F.Fold a b -> b
Field (WrapCV vec) |= f = F.fold f (G.toList vec)
infixl 3 |=

mean :: (Fractional a, ColumnType a) => Column (k ':> a) -> a
mean (Field (WrapCV vec)) = G.sum vec / fromIntegral (G.length vec)
{-# INLINE mean #-}

getTDigest :: KnownNat comp => Column (k ':> Double) -> TD.TDigest comp
getTDigest (Field (WrapCV vec)) = TD.tdigest (G.toList vec)
{-# INLINE getTDigest #-}

median :: Column (k ':> Double) -> Maybe Double
median col = TD.median (getTDigest col :: TD.TDigest 25)

quartiles :: Column (k ':> Double) -> Maybe (Double, Double, Double)
quartiles col = (,,) <$> q 0.25 <*> q 0.5 <*> q 0.75 where
  td = getTDigest col :: TD.TDigest 25
  q = flip TD.quantile td

proxyColumns :: Proxy (KeyValue KnownSymbol ColumnType)
proxyColumns = Proxy

fromCSVStream :: forall xs. (Forall (KeyValue KnownSymbol (Instance1 Csv.FromField (Either Csv.Field))) xs
  , Forall (KeyValue KnownSymbol ColumnType) xs)
  => Csv.DecodeOptions
  -> IO B.ByteString
  -> IO (DataFrame xs)
fromCSVStream opts pop = do
  vecs0 <- hgenerateFor proxyColumns $ const $ Field . WrapCMV 0 <$> MG.new 64
  vecs <- S.thaw vecs0
  let push xs cont = case sequence xs of
        Left err -> fail err
        Right vs -> foldr (\r kont n -> henumerateFor proxyColumns (Proxy :: Proxy xs)
          (\i k -> case getField $ hlookup i r of
            Left f -> fail $ "Conversion failed: " ++ show (f :: Csv.Field)
            Right a -> do
              v <- S.get vecs i
              v' <- pushBack (getField v) a
              S.set vecs i $ Field v'
              k) (kont $! n + 1)
          ) cont vs 0
  let go count (I.Done rs) = push rs $ \n -> do
          content <- hgenerateFor proxyColumns
            $ \i -> fmap (Field . WrapCV) $ S.get vecs i
              >>= \(Field (WrapCMV n v)) -> G.take n <$> G.unsafeFreeze v
          return DataFrame
            { dfContent = content
            , dfLength = count + n
            }
      go count (I.Many rs cont) = push rs $ \n -> pop >>= go (count + n) . cont
      go _ (I.Fail _ err) = fail err
      header (I.DoneH _ p) = go 0 p
      header (I.PartialH f) = pop >>= header . f
      header (I.FailH _ err) = fail err
  header $ I.decodeByNameWith opts

readCSVFile :: (Forall (KeyValue KnownSymbol (Instance1 Csv.FromField (Either Csv.Field))) xs
  , Forall (KeyValue KnownSymbol ColumnType) xs)
  => Csv.DecodeOptions -> FilePath
  -> IO (DataFrame xs)
readCSVFile opts path = do
  h <- openFile path ReadMode
  fromCSVStream opts (B.hGetSome h 4096)
