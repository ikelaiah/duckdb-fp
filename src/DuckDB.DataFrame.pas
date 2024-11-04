unit DuckDB.DataFrame;

{$mode objfpc}{$H+}

interface

uses
  SysUtils, Classes, Variants, libduckdb, Math, TypInfo;

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

  TColumnStats = record
    Count: Integer;
    Mean: Double;
    StdDev: Double;
    Skewness: Double;
    Kurtosis: Double;
    NonMissingRate: Double;
    Min: Variant;
    Q1: Double;
    Median: Double;
    Q3: Double;
    Max: Variant;
    NullCount: Integer;
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
    function IsNumericColumn(const Col: TDuckDBColumn): Boolean;
    function CalculateColumnStats(const Col: TDuckDBColumn): TColumnStats;
    function CalculatePercentile(const Values: array of Double; 
      Percentile: Double): Double;
    
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
    procedure Describe;
    function NullCount: TDuckFrame;
    procedure Info;
  end;

procedure QuickSort(var A: array of Double; iLo, iHi: Integer);

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

function TDuckFrame.IsNumericColumn(const Col: TDuckDBColumn): Boolean;
begin
  Result := Col.DataType in [dctTinyInt, dctSmallInt, dctInteger, dctBigInt, 
                            dctFloat, dctDouble];
end;

function TDuckFrame.CalculateColumnStats(const Col: TDuckDBColumn): TColumnStats;
var
  I: Integer;
  Sum, SumSq, SumCube, SumQuad: Double;
  Value: Double;
  ValidCount: Integer;
  ValidValues: array of Double;
  Mean, Variance: Double;
begin
  Result.Count := FRowCount;
  Result.NullCount := 0;
  Result.Min := Null;
  Result.Max := Null;
  
  // First pass: Count nulls, find min/max, calculate mean
  Sum := 0;
  ValidCount := 0;
  
  for I := 0 to FRowCount - 1 do
  begin
    if VarIsNull(Col.Data[I]) then
      Inc(Result.NullCount)
    else
    begin
      Value := Col.Data[I];
      Inc(ValidCount);
      Sum := Sum + Value;
      
      if VarIsNull(Result.Min) or (Value < Result.Min) then
        Result.Min := Value;
      if VarIsNull(Result.Max) or (Value > Result.Max) then
        Result.Max := Value;
    end;
  end;

  // Calculate mean and non-missing rate
  if ValidCount > 0 then
  begin
    Mean := Sum / ValidCount;
    Result.Mean := Mean;
    Result.NonMissingRate := ValidCount / FRowCount;
  end
  else
  begin
    Result.Mean := 0;
    Result.NonMissingRate := 0;
  end;

  // Second pass: Calculate variance, skewness, and kurtosis
  SumSq := 0;
  SumCube := 0;
  SumQuad := 0;
  
  for I := 0 to FRowCount - 1 do
  begin
    if not VarIsNull(Col.Data[I]) then
    begin
      Value := Col.Data[I] - Mean;
      SumSq := SumSq + Sqr(Value);
      SumCube := SumCube + Value * Value * Value;
      SumQuad := SumQuad + Value * Value * Value * Value;
    end;
  end;

  if ValidCount > 1 then
  begin
    // Calculate variance
    Variance := SumSq / (ValidCount - 1);
    Result.StdDev := Sqrt(Variance);
    
    // Calculate skewness
    if (ValidCount > 2) and (Result.StdDev > 0) then
      Result.Skewness := (SumCube / ValidCount) / Power(Result.StdDev, 3)
    else
      Result.Skewness := 0;
      
    // Calculate kurtosis
    if (ValidCount > 3) and (Result.StdDev > 0) then
      Result.Kurtosis := ((SumQuad / ValidCount) / Sqr(Variance)) - 3  // Excess kurtosis
    else
      Result.Kurtosis := 0;
  end
  else
  begin
    Result.StdDev := 0;
    Result.Skewness := 0;
    Result.Kurtosis := 0;
  end;

  // Calculate quartiles (reuse existing code)
  SetLength(ValidValues, ValidCount);
  ValidCount := 0;
  for I := 0 to FRowCount - 1 do
  begin
    if not VarIsNull(Col.Data[I]) then
    begin
      ValidValues[ValidCount] := Col.Data[I];
      Inc(ValidCount);
    end;
  end;

  if ValidCount > 0 then
  begin
    QuickSort(ValidValues, 0, ValidCount - 1);
    Result.Q1 := CalculatePercentile(ValidValues, 0.25);
    Result.Median := CalculatePercentile(ValidValues, 0.50);
    Result.Q3 := CalculatePercentile(ValidValues, 0.75);
  end
  else
  begin
    Result.Q1 := 0;
    Result.Median := 0;
    Result.Q3 := 0;
  end;
end;

function TDuckFrame.CalculatePercentile(const Values: array of Double; 
  Percentile: Double): Double;
var
  N: Integer;
  Position: Double;
  Lower, Upper: Integer;
  Delta: Double;
begin
  N := Length(Values);
  if N = 0 then
    Exit(0);
  if N = 1 then
    Exit(Values[0]);

  Position := Percentile * (N - 1);
  Lower := Trunc(Position);
  Delta := Position - Lower;
  
  if Lower + 1 >= N then
    Result := Values[N - 1]
  else
    Result := Values[Lower] + Delta * (Values[Lower + 1] - Values[Lower]);
end;

procedure TDuckFrame.Describe;
var
  Col: Integer;
  Stats: TColumnStats;
  ColWidths: array of Integer;
  StatNames: array[0..11] of string = (
    'count', 'mean', 'std', 'skew', 'kurt', 'non-miss',
    'min', '25%', '50%', '75%', 'max', 'null');
  I: Integer;
  MinStr, MaxStr: string;
begin
  if Length(FColumns) = 0 then
  begin
    WriteLn('Empty DataFrame');
    Exit;
  end;

  // Calculate column widths
  SetLength(ColWidths, Length(FColumns) + 1);
  ColWidths[0] := 8; // Width for stat names column

  // Print header
  Write(Format('%-*s ', [ColWidths[0], '']));
  for Col := 0 to Length(FColumns) - 1 do
  begin
    if IsNumericColumn(FColumns[Col]) then
    begin
      ColWidths[Col + 1] := Max(Length(FColumns[Col].Name), 12);
      Write(Format('%-*s ', [ColWidths[Col + 1], FColumns[Col].Name]));
    end;
  end;
  WriteLn;

  // Print statistics for each numeric column
  for I := 0 to High(StatNames) do
  begin
    Write(Format('%-*s ', [ColWidths[0], StatNames[I]]));
    
    for Col := 0 to Length(FColumns) - 1 do
    begin
      if not IsNumericColumn(FColumns[Col]) then
        Continue;

      Stats := CalculateColumnStats(FColumns[Col]);
      
      case I of
        0: Write(Format('%-*d ', [ColWidths[Col + 1], Stats.Count]));
        1: Write(Format('%-*.2f ', [ColWidths[Col + 1], Stats.Mean]));
        2: Write(Format('%-*.2f ', [ColWidths[Col + 1], Stats.StdDev]));
        3: Write(Format('%-*.2f ', [ColWidths[Col + 1], Stats.Skewness]));
        4: Write(Format('%-*.2f ', [ColWidths[Col + 1], Stats.Kurtosis]));
        5: Write(Format('%-*s ', [ColWidths[Col + 1], Format('%.2f%%', [Stats.NonMissingRate * 100])]));
        6: begin
             if VarIsNull(Stats.Min) then
               MinStr := 'NULL'
             else
               MinStr := VarToStr(Stats.Min);
             Write(Format('%-*s ', [ColWidths[Col + 1], MinStr]));
           end;
        7: Write(Format('%-*.2f ', [ColWidths[Col + 1], Stats.Q1]));
        8: Write(Format('%-*.2f ', [ColWidths[Col + 1], Stats.Median]));
        9: Write(Format('%-*.2f ', [ColWidths[Col + 1], Stats.Q3]));
        10: begin
              if VarIsNull(Stats.Max) then
                MaxStr := 'NULL'
              else
                MaxStr := VarToStr(Stats.Max);
              Write(Format('%-*s ', [ColWidths[Col + 1], MaxStr]));
            end;
        11: Write(Format('%-*d ', [ColWidths[Col + 1], Stats.NullCount]));
      end;
    end;
    WriteLn;
  end;
end;

function TDuckFrame.NullCount: TDuckFrame;
var
  Col: Integer;
  Row: Integer;
  NullCounts: array of Integer;
begin
  Result := TDuckFrame.Create;
  try
    SetLength(NullCounts, Length(FColumns));
    
    // Calculate null counts for each column
    for Col := 0 to Length(FColumns) - 1 do
    begin
      NullCounts[Col] := 0;
      for Row := 0 to FRowCount - 1 do
        if VarIsNull(FColumns[Col].Data[Row]) then
          Inc(NullCounts[Col]);
    end;

    // Create result DataFrame with single row
    Result.FRowCount := 1;
    SetLength(Result.FColumns, Length(FColumns));
    
    for Col := 0 to Length(FColumns) - 1 do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := dctInteger;
      SetLength(Result.FColumns[Col].Data, 1);
      Result.FColumns[Col].Data[0] := NullCounts[Col];  // Store as integer
    end;
  except
    Result.Free;
    raise;
  end;
end;

procedure TDuckFrame.Info;
var
  Col: Integer;
  TotalMemory: Int64;
  NullsInColumn: Integer;
  Row: Integer;
begin
  WriteLn(Format('DataFrame: %d rows × %d columns', [FRowCount, Length(FColumns)]));
  WriteLn;
  
  WriteLn('Columns:');
  for Col := 0 to Length(FColumns) - 1 do
  begin
    // Count nulls manually
    NullsInColumn := 0;
    for Row := 0 to FRowCount - 1 do
      if VarIsNull(FColumns[Col].Data[Row]) then
        Inc(NullsInColumn);
        
    WriteLn(Format('  %s: %s (nulls: %d)',
      [FColumns[Col].Name, 
       GetEnumName(TypeInfo(TDuckDBColumnType), Ord(FColumns[Col].DataType)), 
       NullsInColumn]));
  end;
  
  // Estimate memory usage (rough calculation)
  TotalMemory := 0;
  for Col := 0 to Length(FColumns) - 1 do
    TotalMemory := TotalMemory + (FRowCount * SizeOf(Variant));
    
  WriteLn;
  WriteLn(Format('Memory usage: %d bytes (%.2f MB)', 
                 [TotalMemory, TotalMemory / (1024 * 1024)]));
end;

procedure QuickSort(var A: array of Double; iLo, iHi: Integer);
var
  Lo, Hi: Integer;
  Pivot, T: Double;
begin
  Lo := iLo;
  Hi := iHi;
  Pivot := A[(Lo + Hi) div 2];

  repeat
    while A[Lo] < Pivot do Inc(Lo);
    while A[Hi] > Pivot do Dec(Hi);
    if Lo <= Hi then
    begin
      T := A[Lo];
      A[Lo] := A[Hi];
      A[Hi] := T;
      Inc(Lo);
      Dec(Hi);
    end;
  until Lo > Hi;

  if Hi > iLo then QuickSort(A, iLo, Hi);
  if Lo < iHi then QuickSort(A, Lo, iHi);
end;

end. 