{-# LANGUAGE MultiParamTypeClasses, TypeFamilies, TemplateHaskell, DeriveDataTypeable #-}
module Data.Extensible.DataFrame.Time where

import Data.Attoparsec.ByteString
import Data.Csv
import Data.Thyme
import Data.Typeable
import Data.Vector.Unboxed.Deriving
import System.Locale (defaultTimeLocale)

newtype ISO8601 = ISO8601 { unISO8601 :: UTCTime } deriving (Eq, Ord, Typeable)

instance Show ISO8601 where
  showsPrec d = showsPrec d . unISO8601

derivingUnbox "ISO8601" [t|ISO8601 -> UTCTime|] [|unISO8601|] [|ISO8601|]

instance FromField ISO8601 where
  parseField f = do
    bs <- parseField f
    either fail (pure . ISO8601 . buildTime)
        $ parseOnly (timeParser defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ") bs
