unit DuckDB.DataFrame;

{$mode objfpc}{$H+}

interface

uses
  SysUtils, Classes, Variants, libduckdb;

type
  EDuckDBError = class(Exception);

  { Column types supported by DuckDB }
  TDuckDBColumnType = (
    dctUnknown,
    dctBoolean,
    dctTinyInt,
    dctSmallInt,
    dctInteger,
    dctBigInt,
    dctFloat,
    dctDouble,
    dctDate,
    dctTimestamp,
    dctString,
    dctBlob
  );

  { Column information }
  TDuckDBColumn = record
    Name: string;
    DataType: TDuckDBColumnType;
    Data: array of Variant;
  end;

  { DataFrame class for handling query results }
  TDuckFrame = class
  private
    FColumns: array of TDuckDBColumn;
    FRowCount: Integer;
    
    function GetColumnCount: Integer;
    function GetColumn(Index: Integer): TDuckDBColumn;
    function GetColumnByName(const Name: string): TDuckDBColumn;
    function GetValue(Row, Col: Integer): Variant;
    function GetValueByName(Row: Integer; const ColName: string): Variant;
    function FindColumnIndex(const Name: string): Integer;
    function MapDuckDBType(duckdb_type: duckdb_type): TDuckDBColumnType;
    
  public
    constructor Create;
    destructor Destroy; override;
    
    procedure LoadFromResult(AResult: pduckdb_result);
    procedure Clear;
    procedure Print(MaxRows: Integer = 10);
    procedure SaveToCSV(const FileName: string);
    
    property RowCount: Integer read FRowCount;
    property ColumnCount: Integer read GetColumnCount;
    property Columns[Index: Integer]: TDuckDBColumn read GetColumn;
    property ColumnsByName[const Name: string]: TDuckDBColumn read GetColumnByName;
    property Values[Row, Col: Integer]: Variant read GetValue;
    property ValuesByName[Row: Integer; const ColName: string]: Variant read GetValueByName; default;
    
    function Head(Count: Integer = 5): TDuckFrame;
    function Tail(Count: Integer = 5): TDuckFrame;
    function Select(const ColumnNames: array of string): TDuckFrame;
  end;

implementation

{ TDuckFrame }

constructor TDuckFrame.Create;
begin
  inherited Create;
  FRowCount := 0;
  SetLength(FColumns, 0);
end;

destructor TDuckFrame.Destroy;
begin
  Clear;
  inherited;
end;

function TDuckFrame.GetColumnCount: Integer;
begin
  Result := Length(FColumns);
end;

function TDuckFrame.GetColumn(Index: Integer): TDuckDBColumn;
begin
  if (Index < 0) or (Index >= Length(FColumns)) then
    raise EDuckDBError.Create('Column index out of range');
  Result := FColumns[Index];
end;

function TDuckFrame.GetColumnByName(const Name: string): TDuckDBColumn;
var
  Index: Integer;
begin
  Index := FindColumnIndex(Name);
  if Index = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" not found', [Name]);
  Result := FColumns[Index];
end;

function TDuckFrame.GetValue(Row, Col: Integer): Variant;
begin
  if (Row < 0) or (Row >= FRowCount) then
    raise EDuckDBError.Create('Row index out of range');
  if (Col < 0) or (Col >= Length(FColumns)) then
    raise EDuckDBError.Create('Column index out of range');
  Result := FColumns[Col].Data[Row];
end;

function TDuckFrame.GetValueByName(Row: Integer; const ColName: string): Variant;
var
  ColIndex: Integer;
begin
  ColIndex := FindColumnIndex(ColName);
  if ColIndex = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" not found', [ColName]);
  Result := GetValue(Row, ColIndex);
end;

function TDuckFrame.FindColumnIndex(const Name: string): Integer;
var
  I: Integer;
begin
  for I := 0 to Length(FColumns) - 1 do
    if FColumns[I].Name = Name then
      Exit(I);
  Result := -1;
end;

procedure TDuckFrame.Clear;
var
  I: Integer;
begin
  for I := 0 to Length(FColumns) - 1 do
    SetLength(FColumns[I].Data, 0);
  SetLength(FColumns, 0);
  FRowCount := 0;
end;

function TDuckFrame.Tail(Count: Integer = 5): TDuckFrame;
var
  Col, Row, StartRow: Integer;
  RowsToCopy: Integer;
begin
  Result := TDuckFrame.Create;
  try
    if Count > FRowCount then
      RowsToCopy := FRowCount
    else
      RowsToCopy := Count;
      
    StartRow := FRowCount - RowsToCopy;
    if StartRow < 0 then
      StartRow := 0;
      
    Result.FRowCount := RowsToCopy;
    SetLength(Result.FColumns, Length(FColumns));
    
    for Col := 0 to Length(FColumns) - 1 do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := FColumns[Col].DataType;
      SetLength(Result.FColumns[Col].Data, RowsToCopy);
      
      for Row := 0 to RowsToCopy - 1 do
        Result.FColumns[Col].Data[Row] := FColumns[Col].Data[StartRow + Row];
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.Select(const ColumnNames: array of string): TDuckFrame;
var
  I, Col, NewCol: Integer;
  ColIndex: Integer;
begin
  Result := TDuckFrame.Create;
  try
    Result.FRowCount := FRowCount;
    SetLength(Result.FColumns, Length(ColumnNames));
    
    NewCol := 0;
    for I := 0 to Length(ColumnNames) - 1 do
    begin
      ColIndex := FindColumnIndex(ColumnNames[I]);
      if ColIndex = -1 then
        Continue;
        
      Result.FColumns[NewCol].Name := FColumns[ColIndex].Name;
      Result.FColumns[NewCol].DataType := FColumns[ColIndex].DataType;
      SetLength(Result.FColumns[NewCol].Data, FRowCount);
      
      for Col := 0 to FRowCount - 1 do
        Result.FColumns[NewCol].Data[Col] := FColumns[ColIndex].Data[Col];
        
      Inc(NewCol);
    end;
    
    if NewCol = 0 then
    begin
      Result.Free;
      raise EDuckDBError.Create('No valid columns selected');
    end;
    
    SetLength(Result.FColumns, NewCol);
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.MapDuckDBType(duckdb_type: duckdb_type): TDuckDBColumnType;
begin
  case duckdb_type of
    DUCKDB_TYPE_INVALID: Result := dctUnknown;
    DUCKDB_TYPE_BOOLEAN: Result := dctBoolean;
    DUCKDB_TYPE_TINYINT: Result := dctTinyInt;
    DUCKDB_TYPE_SMALLINT: Result := dctSmallInt;
    DUCKDB_TYPE_INTEGER: Result := dctInteger;
    DUCKDB_TYPE_BIGINT: Result := dctBigInt;
    DUCKDB_TYPE_HUGEINT: Result := dctBigInt;
    DUCKDB_TYPE_UTINYINT: Result := dctTinyInt;
    DUCKDB_TYPE_USMALLINT: Result := dctSmallInt;
    DUCKDB_TYPE_UINTEGER: Result := dctInteger;
    DUCKDB_TYPE_UBIGINT: Result := dctBigInt;
    DUCKDB_TYPE_FLOAT: Result := dctFloat;
    DUCKDB_TYPE_DOUBLE: Result := dctDouble;
    DUCKDB_TYPE_TIMESTAMP: Result := dctTimestamp;
    DUCKDB_TYPE_DATE: Result := dctDate;
    DUCKDB_TYPE_TIME: Result := dctString;
    DUCKDB_TYPE_INTERVAL: Result := dctString;
    DUCKDB_TYPE_VARCHAR: Result := dctString;
    DUCKDB_TYPE_BLOB: Result := dctBlob;
    else Result := dctUnknown;
  end;
end;

procedure TDuckFrame.LoadFromResult(AResult: pduckdb_result);
var
  ColCount, Row, Col: Integer;
  StrValue: PAnsiChar;
begin
  Clear;
  
  ColCount := duckdb_column_count(AResult);
  FRowCount := duckdb_row_count(AResult);
  
  SetLength(FColumns, ColCount);
  
  for Col := 0 to ColCount - 1 do
  begin
    // Set column metadata
    FColumns[Col].Name := string(AnsiString(duckdb_column_name(AResult, Col)));
    FColumns[Col].DataType := MapDuckDBType(duckdb_column_type(AResult, Col));
    
    // Allocate space for data
    SetLength(FColumns[Col].Data, FRowCount);
    
    // Load data
    for Row := 0 to FRowCount - 1 do
    begin
      if duckdb_value_is_null(AResult, Col, Row) then
      begin
        FColumns[Col].Data[Row] := Null;
        Continue;
      end;

      case FColumns[Col].DataType of
        dctBoolean:
          FColumns[Col].Data[Row] := duckdb_value_boolean(AResult, Col, Row);
        dctTinyInt:
          FColumns[Col].Data[Row] := duckdb_value_int8(AResult, Col, Row);
        dctSmallInt:
          FColumns[Col].Data[Row] := duckdb_value_int16(AResult, Col, Row);
        dctInteger:
          FColumns[Col].Data[Row] := duckdb_value_int32(AResult, Col, Row);
        dctBigInt:
          FColumns[Col].Data[Row] := duckdb_value_int64(AResult, Col, Row);
        dctFloat:
          FColumns[Col].Data[Row] := duckdb_value_float(AResult, Col, Row);
        dctDouble:
          FColumns[Col].Data[Row] := duckdb_value_double(AResult, Col, Row);
        dctString:
          begin
            StrValue := duckdb_value_varchar(AResult, Col, Row);
            if StrValue <> nil then
            begin
              FColumns[Col].Data[Row] := string(AnsiString(StrValue));
              duckdb_free(StrValue);
            end
            else
              FColumns[Col].Data[Row] := Null;
          end;
        else
          FColumns[Col].Data[Row] := Null;
      end;
    end;
  end;
end;

procedure TDuckFrame.Print(MaxRows: Integer = 10);
var
  Col, Row, ColWidth: Integer;
  StartRow, EndRow: Integer;
  ColWidths: array of Integer;
  S: string;
begin
  if Length(FColumns) = 0 then
  begin
    WriteLn('Empty DataFrame');
    Exit;
  end;

  // Calculate column widths
  SetLength(ColWidths, Length(FColumns));
  for Col := 0 to Length(FColumns) - 1 do
  begin
    ColWidths[Col] := Length(FColumns[Col].Name);
    for Row := 0 to FRowCount - 1 do
    begin
      S := VarToStr(FColumns[Col].Data[Row]);
      if Length(S) > ColWidths[Col] then
        ColWidths[Col] := Length(S);
    end;
  end;

  // Print header
  for Col := 0 to Length(FColumns) - 1 do
    Write(Format('%-*s ', [ColWidths[Col] + 1, FColumns[Col].Name]));
  WriteLn;

  // Print separator
  for Col := 0 to Length(FColumns) - 1 do
  begin
    for ColWidth := 1 to ColWidths[Col] do
      Write('-');
    Write(' ');
  end;
  WriteLn;

  // Determine rows to print
  if (MaxRows > 0) and (FRowCount > MaxRows) then
  begin
    StartRow := 0;
    EndRow := MaxRows div 2;
    
    // Print first half
    for Row := StartRow to EndRow - 1 do
    begin
      for Col := 0 to Length(FColumns) - 1 do
        Write(Format('%-*s ', [ColWidths[Col] + 1, VarToStr(FColumns[Col].Data[Row])]));
      WriteLn;
    end;
    
    WriteLn('...');
    
    // Print last half
    StartRow := FRowCount - (MaxRows div 2);
    EndRow := FRowCount - 1;
    for Row := StartRow to EndRow do
    begin
      for Col := 0 to Length(FColumns) - 1 do
        Write(Format('%-*s ', [ColWidths[Col] + 1, VarToStr(FColumns[Col].Data[Row])]));
      WriteLn;
    end;
    
    WriteLn(Format('[%d rows x %d columns]', [FRowCount, Length(FColumns)]));
  end
  else
  begin
    // Print all rows
    for Row := 0 to FRowCount - 1 do
    begin
      for Col := 0 to Length(FColumns) - 1 do
        Write(Format('%-*s ', [ColWidths[Col] + 1, VarToStr(FColumns[Col].Data[Row])]));
      WriteLn;
    end;
  end;
end;

function TDuckFrame.Head(Count: Integer = 5): TDuckFrame;
var
  Col, Row: Integer;
  RowsToCopy: Integer;
begin
  Result := TDuckFrame.Create;
  try
    RowsToCopy := Count;
    if RowsToCopy > FRowCount then
      RowsToCopy := FRowCount;
      
    Result.FRowCount := RowsToCopy;
    SetLength(Result.FColumns, Length(FColumns));
    
    for Col := 0 to Length(FColumns) - 1 do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := FColumns[Col].DataType;
      SetLength(Result.FColumns[Col].Data, RowsToCopy);
      
      for Row := 0 to RowsToCopy - 1 do
        Result.FColumns[Col].Data[Row] := FColumns[Col].Data[Row];
    end;
  except
    Result.Free;
    raise;
  end;
end;

procedure TDuckFrame.SaveToCSV(const FileName: string);
var
  F: TextFile;
  Row, Col: Integer;
begin
  AssignFile(F, FileName);
  try
    Rewrite(F);
    
    // Write header
    for Col := 0 to Length(FColumns) - 1 do
    begin
      if Col > 0 then
        Write(F, ',');
      Write(F, FColumns[Col].Name);
    end;
    WriteLn(F);
    
    // Write data
    for Row := 0 to FRowCount - 1 do
    begin
      for Col := 0 to Length(FColumns) - 1 do
      begin
        if Col > 0 then
          Write(F, ',');
        Write(F, VarToStr(FColumns[Col].Data[Row]));
      end;
      WriteLn(F);
    end;
  finally
    CloseFile(F);
  end;
end;

end. 