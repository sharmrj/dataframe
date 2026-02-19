module DataFrame.IO.Unstable.Parquet (
    readParquet,
) where

import DataFrame.IO.Unstable.Parquet.Thrift (parseFileMetadata)
import DataFrame.IO.Utils.RandomAccess (mmapFileVector, runReaderIO)
import DataFrame.Internal.DataFrame (DataFrame (..))

readParquet :: FilePath -> IO DataFrame
readParquet filepath = do
    file <- mmapFileVector filepath
    fileMetadata <- runReaderIO parseFileMetadata file
    return undefined
