{-# LANGUAGE TypeFamilies, CPP #-}
module Data.Extensible.DataFrame.Internal where

import qualified Data.ByteString as B
import qualified Data.Text as T
import qualified Data.ByteString.Short as SB
import qualified Data.Text.Short as ST
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Generic.Mutable as MG
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector.Unboxed.Mutable as MU
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import Control.Monad.Primitive
import Data.Thyme (UTCTime)
import Data.Extensible.DataFrame.Time

data family ColumnVector a :: *
data family ColumnIOVector a :: *

pushBackGeneric :: PrimMonad m
  => MG.MVector v a => v (PrimState m) a -> Int
  -> (v (PrimState m) a -> Int -> r) -> a -> m r
pushBackGeneric v n k a
  | MG.length v == n = do
    v' <- MG.unsafeGrow v (n `div` 2)
    MG.unsafeWrite v' n a
    return $ k v' (n + 1)
  | otherwise = k v (n + 1) <$ MG.unsafeWrite v n a

class ColumnType a where
  unsafeIndex :: ColumnVector a -> Int -> a
  unsafeSlice :: Int -> Int -> ColumnVector a -> ColumnVector a
  pushBack :: ColumnIOVector a -> a -> IO (ColumnIOVector a)
  unsafeFreeze :: ColumnIOVector a -> IO (ColumnVector a)
  new :: IO (ColumnIOVector a)

#define CT_Vector(type, con, mcon) \
instance ColumnType (type) where { \
  unsafeIndex (con v) = G.unsafeIndex v; \
  unsafeSlice m n (con v) = con (G.unsafeSlice m n v); \
  pushBack (mcon v n) = pushBackGeneric v n mcon; \
  unsafeFreeze (mcon v _) = con <$> G.unsafeFreeze v; \
  new = do; \
    v <- MG.unsafeNew 64; \
    return $ mcon v 0 }

newtype instance ColumnVector Int = CV_Int (U.Vector Int)
data instance ColumnIOVector Int = CVM_Int (MU.IOVector Int) !Int
CT_Vector(Int, CV_Int, CVM_Int)

newtype instance ColumnVector Float = CV_Float (U.Vector Float)
data instance ColumnIOVector Float = CVM_Float (MU.IOVector Float) !Int
CT_Vector(Float, CV_Float, CVM_Float)

newtype instance ColumnVector Double = CV_Double (U.Vector Double)
data instance ColumnIOVector Double = CVM_Double (MU.IOVector Double) !Int
CT_Vector(Double, CV_Double, CVM_Double)

newtype instance ColumnVector UTCTime = CV_UTCTime (U.Vector UTCTime)
data instance ColumnIOVector UTCTime = CVM_UTCTime (MU.IOVector UTCTime) !Int
CT_Vector(UTCTime, CV_UTCTime, CVM_UTCTime)

newtype instance ColumnVector ISO8601 = CV_ISO8601 (U.Vector ISO8601)
data instance ColumnIOVector ISO8601 = CVM_ISO8601 (MU.IOVector ISO8601) !Int
CT_Vector(ISO8601, CV_ISO8601, CVM_ISO8601)

newtype instance ColumnVector B.ByteString = CV_ByteString (V.Vector B.ByteString)
data instance ColumnIOVector B.ByteString = CVM_ByteString (MV.IOVector B.ByteString) !Int
CT_Vector(B.ByteString, CV_ByteString, CVM_ByteString)

newtype instance ColumnVector SB.ShortByteString = CV_ShortByteString (V.Vector SB.ShortByteString)
data instance ColumnIOVector SB.ShortByteString = CVM_ShortByteString (MV.IOVector SB.ShortByteString) !Int
CT_Vector(SB.ShortByteString, CV_ShortByteString, CVM_ShortByteString)

newtype instance ColumnVector T.Text = CV_Text (V.Vector T.Text)
data instance ColumnIOVector T.Text = CVM_Text (MV.IOVector T.Text) !Int
CT_Vector(T.Text, CV_Text, CVM_Text)

newtype instance ColumnVector ST.ShortText = CV_ShortText (V.Vector ST.ShortText)
data instance ColumnIOVector ST.ShortText = CVM_ShortText (MV.IOVector ST.ShortText) !Int
CT_Vector(ST.ShortText, CV_ShortText, CVM_ShortText)

newtype instance ColumnVector (Maybe a) = CV_Maybe (V.Vector (Maybe a))
data instance ColumnIOVector (Maybe a) = CVM_Maybe (MV.IOVector (Maybe a)) !Int
CT_Vector(Maybe a, CV_Maybe, CVM_Maybe)
