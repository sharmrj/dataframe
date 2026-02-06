{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module DataFrame.IO.Utils.Builder where

import Data.Array.Byte (MutableByteArray)
import DataFrame.IO.Parquet.Types (ParquetType)

data ColumnBuilder s
    = ColumnBuilder
    { elements :: !(MutableByteArray s)
    , length :: !Int
    , pType :: !ParquetType
    }
