{-# LANGUAGE UndecidableInstances, ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-simplifiable-class-constraints #-}
module Data.Extensible.DataFrame
  ( DataFrame(..)
  , headN
  , tailN
  , rowAt
  , columnAt
  , readCSVFile
  ) where

import Prelude hiding (head, tail)
import qualified Data.ByteString as B
import qualified Data.Csv as Csv
import qualified Data.Csv.Incremental as I
import Data.List (intercalate)
import qualified Data.Extensible.DataFrame.Internal as I
import Data.Extensible
import qualified Data.Extensible.Struct as S
import Data.Proxy
import GHC.TypeLits
import Data.Typeable
import System.IO

data DataFrame xs = DataFrame
  { dfContent :: !(RecordOf I.ColumnVector xs)
  , dfLength :: !Int
  }

instance Forall (KeyValue KnownSymbol Typeable) xs => Show (DataFrame xs) where
  show (DataFrame _ count) = intercalate "\n" $ (show count ++ " records of")
    : henumerateFor (Proxy :: Proxy (KeyValue KnownSymbol Typeable)) (Proxy :: Proxy xs)
    (\(p :: Membership xs kv) -> (:) $ symbolVal (proxyAssocKey p)
      ++ " :: " ++ show (typeRep (Proxy :: Proxy (AssocValue kv))))
    []

headN :: Forall (KeyValue KnownSymbol I.ColumnType) xs => DataFrame xs -> Int -> DataFrame xs
headN df n = DataFrame
  { dfContent = htabulateFor proxyColumns $ \p -> Field $ I.unsafeSlice 0 n
      $ getField $ hlookup p $ dfContent df
  , dfLength = n
  }

tailN :: Forall (KeyValue KnownSymbol I.ColumnType) xs => DataFrame xs -> Int -> DataFrame xs
tailN df n = DataFrame
  { dfContent = htabulateFor proxyColumns $ \p -> Field $ I.unsafeSlice (dfLength df - n) n
      $ getField $ hlookup p $ dfContent df
  , dfLength = n
  }

rowAt :: Forall (KeyValue KnownSymbol I.ColumnType) xs
  => DataFrame xs
  -> Int
  -> Record xs
rowAt (DataFrame df len) i
  | i < 0 || i >= len = error $ "rowAt: out of bounds (" ++ show len ++ ")"
  | otherwise = htabulateFor (Proxy :: Proxy (KeyValue KnownSymbol I.ColumnType))
    $ \j -> Field $ pure $ flip I.unsafeIndex i $ getField $ hlookup j df

columnAt :: Associate k v xs
  => DataFrame xs
  -> Proxy k
  -> Field I.ColumnVector (k ':> v)
columnAt (DataFrame df _) _ = hlookup association df

proxyColumns :: Proxy (KeyValue KnownSymbol I.ColumnType)
proxyColumns = Proxy

fromCSVStream :: forall xs. (Forall (KeyValue KnownSymbol (Instance1 Csv.FromField (Either Csv.Field))) xs
  , Forall (KeyValue KnownSymbol I.ColumnType) xs)
  => Csv.DecodeOptions
  -> IO B.ByteString
  -> IO (DataFrame xs)
fromCSVStream opts pop = do
  vecs0 <- hgenerateFor proxyColumns $ const $ Field <$> I.new
  vecs <- S.thaw vecs0
  let push xs cont = case sequence xs of
        Left err -> fail err
        Right vs -> foldr (\r kont n -> henumerateFor proxyColumns (Proxy :: Proxy xs)
          (\i k -> case getField $ hlookup i r of
            Left f -> fail $ "Conversion failed: " ++ show (f :: Csv.Field)
            Right a -> do
              v <- S.get vecs i
              v' <- I.pushBack (getField v) a
              S.set vecs i (Field v')
              k) (kont $! n + 1)
          ) cont vs 0
  let go count (I.Done rs) = push rs $ \n -> do
          content <- hgenerateFor proxyColumns
            $ \i -> fmap Field $ S.get vecs i >>= I.unsafeFreeze . getField
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
  , Forall (KeyValue KnownSymbol I.ColumnType) xs)
  => Csv.DecodeOptions -> FilePath
  -> IO (DataFrame xs)
readCSVFile opts path = do
  h <- openFile path ReadMode
  fromCSVStream opts (B.hGetSome h 4096)
