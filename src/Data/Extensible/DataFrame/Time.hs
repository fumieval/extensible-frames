{-# LANGUAGE MultiParamTypeClasses, TypeFamilies, TemplateHaskell, DeriveDataTypeable, ScopedTypeVariables #-}
module Data.Extensible.DataFrame.Time where

import Data.Attoparsec.ByteString
import Data.Csv
import Data.Proxy
import Data.Thyme
import Data.Typeable
import Data.Vector.Unboxed.Deriving
import GHC.TypeLits
import System.Locale (defaultTimeLocale)

newtype TimeAs format = TimeAs { unTimeAs :: UTCTime } deriving (Eq, Ord, Typeable)

instance KnownSymbol fmt => Show (TimeAs fmt) where
  showsPrec d t = showsPrec d
    $ formatTime defaultTimeLocale
      (symbolVal t ) (unTimeAs t)

derivingUnbox "TimeAs" [t|forall fmt. TimeAs fmt -> UTCTime|] [|unTimeAs|] [|TimeAs|]

type ISO8601 = "%Y-%m-%dT%H:%M:%S%QZ"

instance KnownSymbol fmt => FromField (TimeAs fmt) where
  parseField f = do
    bs <- parseField f
    let fmt = symbolVal (Proxy :: Proxy fmt)
    either fail (pure . TimeAs . buildTime)
        $ parseOnly (timeParser defaultTimeLocale fmt) bs
