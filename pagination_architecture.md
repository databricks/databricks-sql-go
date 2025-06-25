# Databricks SQL Go Driver - Pagination Architecture

## UML Class Diagram

```mermaid
classDiagram
    %% Interfaces
    class ResultPageIterator {
        <<interface>>
        +Next() (TFetchResultsResp, error)
        +HasNext() bool
        +Close() error
        +Start() int64
        +End() int64
        +Count() int64
    }

    class RawBatchIterator {
        <<interface>>
        +Next() (TSparkArrowBatch, error)
        +HasNext() bool
        +Close()
        +GetStartRowOffset() int64
    }

    class BatchIterator {
        <<interface>>
        +Next() (SparkArrowBatch, error)
        +HasNext() bool
        +Close()
    }

    class IPCStreamIterator {
        <<interface>>
        +NextIPCStream() (io.Reader, error)
        +HasNext() bool
        +Close()
        +GetSchemaBytes() ([]byte, error)
    }

    class ArrowBatchIterator {
        <<interface>>
        +Next() (arrow.Record, error)
        +HasNext() bool
        +Close()
        +Schema() (*arrow.Schema, error)
    }

    %% Concrete Implementations
    class resultPageIterator {
        -opHandle: TOperationHandle
        -client: TCLIService
        -maxPageSize: int64
        -isFinished: bool
        -nextResultPage: TFetchResultsResp
        -closedOnServer: bool
        +getNextPage() (TFetchResultsResp, error)
    }

    class pagedRawBatchIterator {
        -resultPageIterator: ResultPageIterator
        -currentIterator: RawBatchIterator
        -cfg: Config
        +Next() (TSparkArrowBatch, error)
        +HasNext() bool
    }

    class localRawBatchIterator {
        -batches: []TSparkArrowBatch
        -index: int
        -currentRowOffset: int64
        +Next() (TSparkArrowBatch, error)
        +HasNext() bool
    }

    class cloudRawBatchIterator {
        -pendingLinks: Queue[TSparkArrowResultLink]
        -downloadTasks: Queue[downloadTask]
        -currentRowOffset: int64
        +Next() (TSparkArrowBatch, error)
        +HasNext() bool
    }

    class batchIterator {
        -rawIterator: RawBatchIterator
        -arrowSchemaBytes: []byte
        -cfg: Config
        +Next() (SparkArrowBatch, error)
        +HasNext() bool
    }

    class ipcStreamIterator {
        -rawBatchIterator: RawBatchIterator
        -arrowSchemaBytes: []byte
        -useLz4: bool
        +NextIPCStream() (io.Reader, error)
        +HasNext() bool
    }

    class arrowRecordIterator {
        -resultPageIterator: ResultPageIterator
        -batchIterator: BatchIterator
        -currentBatch: SparkArrowBatch
        -currentRecord: SparkArrowRecord
        -arrowSchemaBytes: []byte
        -cfg: Config
        +Next() (arrow.Record, error)
        +HasNext() bool
        +Schema() (*arrow.Schema, error)
        +newBatchIterator(resp) BatchIterator
    }

    %% Relationships
    ResultPageIterator <|.. resultPageIterator : implements
    RawBatchIterator <|.. pagedRawBatchIterator : implements
    RawBatchIterator <|.. localRawBatchIterator : implements
    RawBatchIterator <|.. cloudRawBatchIterator : implements
    BatchIterator <|.. batchIterator : implements
    IPCStreamIterator <|.. ipcStreamIterator : implements
    ArrowBatchIterator <|.. arrowRecordIterator : implements

    pagedRawBatchIterator --> ResultPageIterator : uses
    pagedRawBatchIterator --> RawBatchIterator : creates
    batchIterator --> RawBatchIterator : wraps
    ipcStreamIterator --> RawBatchIterator : uses
    
    arrowRecordIterator --> ResultPageIterator : uses
    arrowRecordIterator --> BatchIterator : creates per page
    arrowRecordIterator --> localRawBatchIterator : creates via factory
    arrowRecordIterator --> cloudRawBatchIterator : creates via factory

    %% External dependencies
    class TCLIService {
        <<external>>
        +FetchResults(req) (TFetchResultsResp)
    }

    class TFetchResultsResp {
        <<thrift>>
        +Results: TRowSet
        +HasMoreRows: bool
    }

    class TRowSet {
        <<thrift>>
        +ArrowBatches: []TSparkArrowBatch
        +ResultLinks: []TSparkArrowResultLink
        +StartRowOffset: int64
    }

    resultPageIterator --> TCLIService : calls
    resultPageIterator --> TFetchResultsResp : returns
    TFetchResultsResp --> TRowSet : contains
```

## Sequence Diagram - How Pagination Works

```mermaid
sequenceDiagram
    participant Client
    participant IPCStreamIterator
    participant PagedRawBatchIterator
    participant ResultPageIterator
    participant LocalRawBatchIterator
    participant Server

    Client->>IPCStreamIterator: NextIPCStream()
    IPCStreamIterator->>PagedRawBatchIterator: Next()
    
    alt currentIterator is null or exhausted
        PagedRawBatchIterator->>ResultPageIterator: HasNext()
        
        alt nextResultPage is null
            ResultPageIterator->>ResultPageIterator: getNextPage()
            ResultPageIterator->>Server: FetchResults(opHandle, maxRows)
            Server-->>ResultPageIterator: TFetchResultsResp
            ResultPageIterator->>ResultPageIterator: cache in nextResultPage
        end
        
        ResultPageIterator-->>PagedRawBatchIterator: true
        PagedRawBatchIterator->>ResultPageIterator: Next()
        ResultPageIterator-->>PagedRawBatchIterator: TFetchResultsResp (cached)
        
        alt has ArrowBatches
            PagedRawBatchIterator->>LocalRawBatchIterator: new(batches)
        else has ResultLinks
            PagedRawBatchIterator->>CloudRawBatchIterator: new(links)
        end
        
        PagedRawBatchIterator->>LocalRawBatchIterator: Next()
        LocalRawBatchIterator-->>PagedRawBatchIterator: TSparkArrowBatch
    else currentIterator has more
        PagedRawBatchIterator->>LocalRawBatchIterator: Next()
        LocalRawBatchIterator-->>PagedRawBatchIterator: TSparkArrowBatch
    end
    
    PagedRawBatchIterator-->>IPCStreamIterator: TSparkArrowBatch
    IPCStreamIterator->>IPCStreamIterator: wrap with schema
    IPCStreamIterator-->>Client: io.Reader (IPC stream)
```

## Component Interaction Flow

```mermaid
flowchart TB
    subgraph Server Side
        DB[(Database)]
        API[Thrift API<br/>FetchResults]
    end
    
    subgraph Client Side - Pagination Layer
        RPI[ResultPageIterator]
        RPI -->|fetches pages| API
        RPI -->|pre-fetches on HasNext| Cache[Page Cache]
    end
    
    subgraph Client Side - Raw Batch Layer
        PRBI[PagedRawBatchIterator]
        LRBI[LocalRawBatchIterator]
        CRBI[CloudRawBatchIterator]
        
        PRBI -->|uses| RPI
        PRBI -->|creates per page| LRBI
        PRBI -->|creates per page| CRBI
    end
    
    subgraph Client Side - Processing Layer
        BI[BatchIterator]
        IPCI[IPCStreamIterator]
        ARI[ArrowRecordIterator]
        
        BI -->|wraps| LRBI
        BI -->|wraps| CRBI
        IPCI -->|uses| PRBI
        ARI -->|uses| RPI
        ARI -->|creates| BI
    end
    
    subgraph Client Side - User API
        Rows[rows.GetIPCStreams()]
        ArrowAPI[rows.GetArrowBatches()]
        
        Rows -->|creates| IPCI
        ArrowAPI -->|creates| ARI
    end
```

## ArrowRecordIterator Flow (New Architecture)

```mermaid
sequenceDiagram
    participant Client
    participant ArrowRecordIterator
    participant ResultPageIterator
    participant RawBatchIterator
    participant BatchIterator
    participant Server

    Client->>ArrowRecordIterator: Next()
    
    alt batchIterator is null or exhausted
        ArrowRecordIterator->>ResultPageIterator: Next()
        ResultPageIterator->>Server: FetchResults()
        Server-->>ResultPageIterator: TFetchResultsResp
        ResultPageIterator-->>ArrowRecordIterator: TFetchResultsResp
        
        alt has ArrowBatches
            ArrowRecordIterator->>localRawBatchIterator: NewLocalRawBatchIterator(batches)
            ArrowRecordIterator->>BatchIterator: NewBatchIterator(rawIterator, schema)
        else has ResultLinks  
            ArrowRecordIterator->>cloudRawBatchIterator: NewCloudRawBatchIterator(links)
            ArrowRecordIterator->>BatchIterator: NewBatchIterator(rawIterator, schema)
        end
        
        ArrowRecordIterator->>ArrowRecordIterator: batchIterator = new BatchIterator
    end
    
    alt currentBatch is null or exhausted
        ArrowRecordIterator->>BatchIterator: Next()
        BatchIterator->>RawBatchIterator: Next()
        RawBatchIterator-->>BatchIterator: TSparkArrowBatch
        BatchIterator->>BatchIterator: Parse Arrow data
        BatchIterator-->>ArrowRecordIterator: SparkArrowBatch
        ArrowRecordIterator->>ArrowRecordIterator: currentBatch = batch
    end
    
    ArrowRecordIterator->>SparkArrowBatch: Next()
    SparkArrowBatch-->>ArrowRecordIterator: SparkArrowRecord
    ArrowRecordIterator-->>Client: arrow.Record
```

## Key Design Patterns

1. **Iterator Pattern**: Multiple levels of iterators, each handling a specific concern
2. **Decorator Pattern**: BatchIterator decorates RawBatchIterator with parsing
3. **Strategy Pattern**: Different strategies for local vs cloud batch fetching
4. **Lazy Loading**: Pages fetched only when needed
5. **Pre-fetching**: HasNext() pre-fetches to check availability without consuming

## Benefits of This Architecture

- **Separation of Concerns**: Each layer handles one responsibility
- **Flexibility**: Easy to add new iterator types or change implementations
- **Performance**: Pre-fetching and lazy loading optimize network calls
- **Reusability**: Raw batch iterators can be used by multiple consumers