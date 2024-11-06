unit DuckDB.Wrapper;

{$mode objfpc}{$H+}{$J-}

interface

uses
  SysUtils, Classes, libduckdb, DuckDB.DataFrame;

type
  EDuckDBError = class(Exception);

  { TDuckDBConnection }
  TDuckDBConnection = class
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
    function Query(const ASQL: string): TDuckFrame;
    function QueryValue(const ASQL: string): Variant;
    
    // Transaction management
    procedure BeginTransaction;
    procedure Commit;
    procedure Rollback;
    
    class function ReadCSV(const FileName: string): TDuckFrame;
  end;

implementation

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

function TDuckDBConnection.Clone: TDuckDBConnection;
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
end. 
