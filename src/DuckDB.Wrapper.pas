unit DuckDB.Wrapper;

{$mode objfpc}{$H+}{$J-}
{$modeswitch advancedrecords}

interface

uses
  // Only need DuckDB.Base for interfaces
  SysUtils, Classes, libduckdb, DuckDB.Base, Math, Variants;

type

  { TDuckDBConnection }
  TDuckDBConnection = class(TInterfacedObject, IDuckDBConnection)
  private
    FDatabase: p_duckdb_database;
    FConnection: p_duckdb_connection;
    FDatabasePath: string;
    FIsConnected: Boolean;
    
    procedure CheckError(AState: duckdb_state; const AMessage: string);
    procedure RaiseIfNotConnected;
  public
    constructor Create; overload;
    constructor Create(const ADatabasePath: string); overload;
    destructor Destroy; override;
    
    // Connection management
    procedure Open(const ADatabasePath: string = '');
    procedure Close;
    function Clone: TDuckDBConnection;
    property IsConnected: Boolean read FIsConnected;
    property DatabasePath: string read FDatabasePath;
    
    // Query execution
    procedure ExecuteSQL(const ASQL: string);
    function Query(const ASQL: string): IDuckFrame;
    function QueryValue(const ASQL: string): Variant;
    
    // Transaction management
    procedure BeginTransaction;
    procedure Commit;
    procedure Rollback;
    
    function ReadCSV(const FileName: string): TDuckFrame;
    procedure WriteToTable(const DataFrame: IDuckFrame; const TableName: string; 
      const SchemaName: string = 'main');
  end;

// Move function declaration to interface section
function GetDuckDBTypeString(ColumnType: TDuckDBColumnType): string;

implementation

uses
  { 
    - DuckDB.Wrapper needs DuckDB.DataFrame in implementation 
      to create TDuckFrame instances
    - DuckDB.DataFrame does NOT need DuckDB.Wrapper in implementation 
      because it uses TDuckDBConnection through its interface 
      IDuckDBConnection
  }
  DuckDB.DataFrame;  // Only needed in implementation

// Helper function for boolean to string conversion
function BoolToString(const Value: Boolean): string;
begin
  if Value then
    Result := 'TRUE'
  else
    Result := 'FALSE';
end;

{ TDuckDBConnection }

constructor TDuckDBConnection.Create;
begin
  inherited Create;
  FIsConnected := False;
  FDatabase := nil;
  FConnection := nil;
end;

constructor TDuckDBConnection.Create(const ADatabasePath: string);
begin
  Create;
  Open(ADatabasePath);
end;

destructor TDuckDBConnection.Destroy;
begin
  Close;
  inherited Destroy;
end;

procedure TDuckDBConnection.CheckError(AState: duckdb_state; const AMessage: string);
begin
  if AState = DuckDBError then
    raise EDuckDBError.Create(AMessage);
end;

procedure TDuckDBConnection.RaiseIfNotConnected;
begin
  if not FIsConnected then
    raise EDuckDBError.Create('Not connected to database');
end;

procedure TDuckDBConnection.Open(const ADatabasePath: string = '');
var
  State: duckdb_state;
  DBPath: PAnsiChar;
begin
  Close;
  
  FDatabasePath := ADatabasePath;
  
  if ADatabasePath = '' then
    DBPath := nil
  else
    DBPath := PAnsiChar(AnsiString(ADatabasePath));
    
  State := duckdb_open(DBPath, @FDatabase);
  CheckError(State, 'Failed to open database');
  
  State := duckdb_connect(FDatabase, @FConnection);
  CheckError(State, 'Failed to create connection');
  
  FIsConnected := True;
end;

procedure TDuckDBConnection.Close;
begin
  if FConnection <> nil then
  begin
    duckdb_disconnect(@FConnection);
    FConnection := nil;
  end;
  
  if FDatabase <> nil then
  begin
    duckdb_close(@FDatabase);
    FDatabase := nil;
  end;
  
  FIsConnected := False;
end;

function TDuckDBConnection.Clone: IDuckDBConnection;
var
  State: duckdb_state;
begin
  RaiseIfNotConnected;
  
  Result := TDuckDBConnection.Create;
  try
    Result.FDatabase := FDatabase;
    State := duckdb_connect(FDatabase, @Result.FConnection);
    Result.CheckError(State, 'Failed to clone connection');
    Result.FDatabasePath := FDatabasePath;
    Result.FIsConnected := True;
  except
    Result.Free;
    raise;
  end;
end;

procedure TDuckDBConnection.ExecuteSQL(const ASQL: string);
var
  QueryResult: duckdb_result;
  State: duckdb_state;
begin
  RaiseIfNotConnected;
  
  State := duckdb_query(FConnection, PAnsiChar(AnsiString(ASQL)), @QueryResult);
  try
    CheckError(State, 'Failed to execute SQL: ' + ASQL);
  finally
    duckdb_destroy_result(@QueryResult);
  end;
end;

function TDuckDBConnection.Query(const ASQL: string): TDuckFrame;
var
  DuckResult: duckdb_result;
  State: duckdb_state;
begin
  RaiseIfNotConnected;
  
  Result := TDuckFrame.Create;
  try
    State := duckdb_query(FConnection, PAnsiChar(AnsiString(ASQL)), @DuckResult);
    CheckError(State, 'Query failed: ' + ASQL);
    
    try
      Result.LoadFromResult(@DuckResult);
    finally
      duckdb_destroy_result(@DuckResult);
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckDBConnection.QueryValue(const ASQL: string): Variant;
var
  DF: TDuckFrame;
begin
  DF := Query(ASQL);
  try
    if (DF.RowCount = 0) or (DF.ColumnCount = 0) then
      Result := Null
    else
      Result := DF.Values[0, 0];
  finally
    DF.Free;
  end;
end;

procedure TDuckDBConnection.BeginTransaction;
begin
  ExecuteSQL('BEGIN TRANSACTION');
end;

procedure TDuckDBConnection.Commit;
begin
  ExecuteSQL('COMMIT');
end;

procedure TDuckDBConnection.Rollback;
begin
  ExecuteSQL('ROLLBACK');
end;

class function TDuckDBConnection.ReadCSV(const FileName: string): TDuckFrame;
var
  SQLQuery: string;
  DB: TDuckDBConnection;
begin
  if not FileExists(FileName) then
    raise EDuckDBError.CreateFmt('File not found: %s', [FileName]);

  DB := TDuckDBConnection.Create;
  try
    DB.Open;
    
    SQLQuery := Format('SELECT * FROM read_csv_auto(''%s'')', [
      StringReplace(FileName, '''', '''''', [rfReplaceAll])
    ]);
    
    Result := DB.Query(SQLQuery);
  finally
    DB.Free;
  end;
end;

procedure TDuckDBConnection.WriteToTable(const DataFrame: TDuckFrame; const TableName: string; 
  const SchemaName: string = 'main');
var
  SQL: string;
  Col: Integer;
  Row: Integer;
  Value: Variant;
  VType: Integer;
  
  // Helper function to properly escape and quote values
  function EscapeValue(const V: Variant): string;
  begin
    if VarIsNull(V) then
      Exit('NULL');
      
    VType := VarType(V);
    if (VType = varString) or (VType = varUString) or (VType = varOleStr) then
      Result := '''' + StringReplace(VarToStr(V), '''', '''''', [rfReplaceAll]) + ''''
    else if VType = varBoolean then
      Result := BoolToString(Boolean(V))  // Use helper function instead of IfThen
    else
      Result := VarToStr(V);
  end;
  
begin
  RaiseIfNotConnected;
  
  if DataFrame.ColumnCount = 0 then
    raise EDuckDBError.Create('Cannot write empty DataFrame to table');
    
  // Create table if not exists
  SQL := Format('CREATE TABLE IF NOT EXISTS %s.%s (', [SchemaName, TableName]);
  
  for Col := 0 to DataFrame.ColumnCount - 1 do
  begin
    if Col > 0 then
      SQL := SQL + ', ';
    SQL := SQL + Format('%s %s', [
      DataFrame.Columns[Col].Name,
      GetDuckDBTypeString(DataFrame.Columns[Col].DataType)
    ]);
  end;
  SQL := SQL + ');';
  
  ExecuteSQL(SQL);
  
  // Insert data in batches
  BeginTransaction;
  try
    for Row := 0 to DataFrame.RowCount - 1 do
    begin
      SQL := Format('INSERT INTO %s.%s VALUES (', [SchemaName, TableName]);
      
      for Col := 0 to DataFrame.ColumnCount - 1 do
      begin
        if Col > 0 then
          SQL := SQL + ', ';
        Value := DataFrame.Values[Row, Col];
        SQL := SQL + EscapeValue(Value);
      end;
      
      SQL := SQL + ');';
      ExecuteSQL(SQL);
    end;
    
    Commit;
  except
    Rollback;
    raise;
  end;
end;

// Move implementation of GetDuckDBTypeString here
function GetDuckDBTypeString(ColumnType: TDuckDBColumnType): string;
begin
  case ColumnType of
    dctBoolean: Result := 'BOOLEAN';
    dctTinyInt: Result := 'TINYINT';
    dctSmallInt: Result := 'SMALLINT';
    dctInteger: Result := 'INTEGER';
    dctBigInt: Result := 'BIGINT';
    dctFloat: Result := 'FLOAT';
    dctDouble: Result := 'DOUBLE';
    dctDate: Result := 'DATE';
    dctTimestamp: Result := 'TIMESTAMP';
    dctString: Result := 'VARCHAR';
    dctBlob: Result := 'BLOB';
    else Result := 'VARCHAR';  // Default to VARCHAR for unknown types
  end;
end;
end. 
