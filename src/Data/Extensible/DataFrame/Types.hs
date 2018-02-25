{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
module Data.Extensible.DataFrame.Types where

import Control.Monad.Primitive
import Data.Extensible
import Data.Extensible.DataFrame.Time
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

class (G.Vector (ColumnVector a) a, MG.MVector (G.Mutable (ColumnVector a)) a) => ColumnType a where
  type ColumnVector a :: * -> *

newtype WrapCV a = WrapCV
  { unwrapCV :: ColumnVector a a }

data WrapCMV s a = WrapCMV !Int !(G.Mutable (ColumnVector a) s a)

pushBack :: (PrimMonad m, MG.MVector (G.Mutable (ColumnVector a)) a)
  => WrapCMV (PrimState m) a -> a -> m (WrapCMV (PrimState m) a)
pushBack (WrapCMV n v) a
  | MG.length v == n = do
    v' <- MG.unsafeGrow v (n `div` 2)
    MG.unsafeWrite v' n a
    return $ WrapCMV (n + 1) v'
  | otherwise = WrapCMV (n + 1) v <$ MG.unsafeWrite v n a

-- | Ascending order
newtype Asc a = Asc { unAsc :: a } deriving (Show, Eq, Ord)

derivingUnbox "Asc" [t|forall a. MU.Unbox a => Asc a -> a|] [|unAsc|] [|Asc|]

instance (ColumnType a, G.Vector (ColumnVector a) (Asc a)) => ColumnType (Asc a) where
  type ColumnVector (Asc a) = ColumnVector a

instance ColumnType Int where
  type ColumnVector Int = U.Vector

instance ColumnType Double where
  type ColumnVector Double = U.Vector

instance ColumnType UTCTime where
  type ColumnVector UTCTime = U.Vector

instance ColumnType (TimeAs fmt) where
  type ColumnVector (TimeAs fmt) = U.Vector

instance ColumnType B.ByteString where
  type ColumnVector B.ByteString = V.Vector

instance ColumnType T.Text where
  type ColumnVector T.Text = V.Vector

instance ColumnType SB.ShortByteString where
  type ColumnVector SB.ShortByteString = V.Vector

instance ColumnType ST.ShortText where
  type ColumnVector ST.ShortText = V.Vector

data DataFrame xs = DataFrame
  { dfContent :: !(RecordOf WrapCV xs)
  , dfLength :: !Int
  }

class (Show a, ColumnType a) => DisplayColumn a
instance (Show a, ColumnType a) => DisplayColumn a

instance Forall (KeyValue KnownSymbol DisplayColumn) xs => Show (DataFrame xs) where
  show (DataFrame df count) = intercalate "\n"
    $ intercalate "\t" (henumerateFor
      (Proxy :: Proxy (KeyValue KnownSymbol DisplayColumn)) (Proxy :: Proxy xs)
      (\(p :: Membership xs kv) -> (:) $ symbolVal (proxyAssocKey p))
      [])
    : [intercalate "\t" $ henumerateFor
      (Proxy :: Proxy (KeyValue KnownSymbol DisplayColumn)) (Proxy :: Proxy xs)
      (\p -> (:) $ show $ flip G.unsafeIndex i $ unwrapCV $ getField $ hlookup p df)
    [] | i <- [0..count - 1]]

type Column = Field WrapCV
