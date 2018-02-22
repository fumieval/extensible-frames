{-# LANGUAGE MultiParamTypeClasses, TypeFamilies, TemplateHaskell #-}
module Data.Extensible.DataFrame.Time where

import Data.Attoparsec.ByteString
import Data.Csv
import Data.Thyme
import Data.Vector.Unboxed.Deriving
import System.Locale (defaultTimeLocale)

newtype ISO8601 = ISO8601 { getISO8601 :: UTCTime }

derivingUnbox "ISO8601" [t|ISO8601 -> UTCTime|] [|getISO8601|] [|ISO8601|]

instance FromField ISO8601 where
  parseField f = do
    bs <- parseField f
    either fail (pure . ISO8601 . buildTime)
        $ parseOnly (timeParser defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ") bs
