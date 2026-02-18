{-# LANGUAGE OverloadedStrings #-}

module UnstableParquet where

import qualified Data.ByteString as BS
import Data.Bits (shiftL, (.|.))
import Test.HUnit

import DataFrame.IO.Parquet.Thrift (readMetadata)
import DataFrame.IO.Unstable.Parquet.Thrift (parseFileMetadata)
import DataFrame.IO.Utils.RandomAccess (RandomAccess (..), mmapFileVector, runReaderIO, unsafeToByteString)

metadataSizeFromFooter :: BS.ByteString -> Int
metadataSizeFromFooter footer =
    let sizeBytes :: [Int]
        sizeBytes = map (fromIntegral . BS.index footer) [0 .. 3]
     in foldl (.|.) 0 $ zipWith shiftL sizeBytes [0, 8, 16, 24]

unstableMatchesOld :: FilePath -> IO ()
unstableMatchesOld path = do
    vec <- mmapFileVector path
    let run = runReaderIO
    footer <- run (readSuffix 8) vec
    let size = metadataSizeFromFooter footer
    suffixBytes <- run (readSuffix (size + 8)) vec
    let metaBytes = BS.take size suffixBytes
        contents = unsafeToByteString vec
    oldMeta <- readMetadata contents size
    case parseFileMetadata metaBytes of
        Left e -> assertFailure $ "parseFileMetadata failed: " ++ show e
        Right newMeta -> assertEqual path oldMeta newMeta

allTypesPlainTest :: Test
allTypesPlainTest = TestCase $ unstableMatchesOld "./tests/data/alltypes_plain.parquet"

allTypesPlainSnappyTest :: Test
allTypesPlainSnappyTest = TestCase $ unstableMatchesOld "./tests/data/alltypes_plain.snappy.parquet"

allTypesDictionaryTest :: Test
allTypesDictionaryTest = TestCase $ unstableMatchesOld "./tests/data/alltypes_dictionary.parquet"

mtCarsTest :: Test
mtCarsTest = TestCase $ unstableMatchesOld "./tests/data/mtcars.parquet"

tests :: [Test]
tests = [allTypesPlainTest, allTypesPlainSnappyTest, allTypesDictionaryTest, mtCarsTest]
