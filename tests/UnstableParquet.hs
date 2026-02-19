{-# LANGUAGE OverloadedStrings #-}

module UnstableParquet where

import Data.Bits (shiftL, (.|.))
import qualified Data.ByteString as BS
import Test.HUnit

import DataFrame.IO.Parquet (readMetadataFromPath)
import DataFrame.IO.Unstable.Parquet.Thrift (parseFileMetadata)
import DataFrame.IO.Utils.RandomAccess (
    RandomAccess (..),
    mmapFileVector,
    runReaderIO,
    unsafeToByteString,
 )

unstableMatchesOld :: FilePath -> IO ()
unstableMatchesOld path = do
    vec <- mmapFileVector path
    oldMeta <- readMetadataFromPath path
    newMeta <- runReaderIO parseFileMetadata vec
    assertEqual path oldMeta newMeta

allTypesPlainTest :: Test
allTypesPlainTest = TestCase $ unstableMatchesOld "./tests/data/alltypes_plain.parquet"

allTypesPlainSnappyTest :: Test
allTypesPlainSnappyTest = TestCase $ unstableMatchesOld "./tests/data/alltypes_plain.snappy.parquet"

allTypesDictionaryTest :: Test
allTypesDictionaryTest = TestCase $ unstableMatchesOld "./tests/data/alltypes_dictionary.parquet"

mtCarsTest :: Test
mtCarsTest = TestCase $ unstableMatchesOld "./tests/data/mtcars.parquet"

tests :: [Test]
tests =
    [allTypesPlainTest, allTypesPlainSnappyTest, allTypesDictionaryTest, mtCarsTest]
