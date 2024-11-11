unit DuckDB.FreePascal.Cases;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, fpcunit, testutils, testregistry, DuckDB.DataFrame,
  Variants, DuckDB.Wrapper, DateUtils;

type
  { TDuckDBDataFrameTest }
  TDuckDBDataFrameTest = class(TTestCase)
  private
    FTempDir: string;
    FDBPath: string;

    // Helper methods
    procedure CreateSampleCSV(const FileName: string; const HasHeaders: boolean);
    procedure CreateSampleCSVWithDelimiter(const FileName: string;
      const Delimiter: char);
    procedure CreateSampleDatabase;
    function CreateSampleFrame: TDuckFrame;
    function CreateNumericFrame: TDuckFrame;
    procedure AssertFrameEquals(Expected, Actual: TDuckFrame; const Msg: string = '');
    procedure DeleteDirectoryRecursively(const DirName: string);

  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    // Constructor Tests
    procedure TestCreateBlank;
    procedure TestCreateBlankMismatchedArrays;
    procedure TestCreateFromCSVWithHeaders;
    procedure TestCreateFromCSVWithoutHeaders;
    procedure TestCreateFromCSVWithDifferentDelimiter;
    procedure TestCreateFromDuckDB;

    // Basic Operations
    procedure TestAddColumn;
    procedure TestAddRow;
    procedure TestSetValue;
    procedure TestClear;
    procedure TestGetColumnByName;
    procedure TestGetColumnNames;
    procedure TestFindColumnIndex;
    procedure TestDateTimeArrays;

    // Data Access
    procedure TestHead;
    procedure TestValuesByName;
    procedure TestValues;

    // Data Manipulation
    procedure TestUnion;
    procedure TestUnionAll;
    procedure TestUnionWithDifferentColumns;
    procedure TestDistinct;
    procedure TestDropNA;
    procedure TestFillNA;

    // Statistical Operations
    procedure TestDescribe;
    procedure TestNullCount;
    procedure TestCorrPearson;
    procedure TestCorrSpearman;
    procedure TestCalculateColumnStats;
    procedure TestCalculatePercentile;

    // Plotting
    procedure TestPlotHistogram;

    // CSV Operations
    procedure TestSaveToCSV;

    // Error Handling
    procedure TestInvalidColumnAccess;
    procedure TestInvalidRowAccess;
    procedure TestInvalidCSVFile;

    // Type Conversion
    procedure TestTryConvertValue;
    procedure TestTypeConversion;

    // Print and Info
    procedure TestPrint;
    procedure TestInfo;
  end;

implementation

{ Helper Methods }

procedure TDuckDBDataFrameTest.SetUp;
begin
  inherited;
  FTempDir := GetTempDir + 'DuckDBTest' + PathDelim;
  if not DirectoryExists(FTempDir) then
    CreateDir(FTempDir);
  FDBPath := '';
end;

procedure TDuckDBDataFrameTest.TearDown;
begin
  if FileExists(FDBPath) then
    DeleteFile(FDBPath);
  if DirectoryExists(FTempDir) then
    DeleteDirectoryRecursively(FTempDir);
  inherited;
end;

procedure TDuckDBDataFrameTest.DeleteDirectoryRecursively(const DirName: string);
var
  SearchRec: TSearchRec;
  FindResult: integer;
begin
  FindResult := FindFirst(IncludeTrailingPathDelimiter(DirName) + '*',
    faAnyFile, SearchRec);
  try
    while FindResult = 0 do
    begin
      if (SearchRec.Name <> '.') and (SearchRec.Name <> '..') then
      begin
        if (SearchRec.Attr and faDirectory) <> 0 then
          DeleteDirectoryRecursively(IncludeTrailingPathDelimiter(DirName) +
            SearchRec.Name)
        else
          DeleteFile(IncludeTrailingPathDelimiter(DirName) + SearchRec.Name);
      end;
      FindResult := FindNext(SearchRec);
    end;
  finally
    FindClose(SearchRec);
  end;
  RemoveDir(DirName);
end;

function TDuckDBDataFrameTest.CreateSampleFrame: TDuckFrame;
begin
  Result := TDuckFrame.CreateBlank(['Name', 'Age', 'City', 'Salary'],
    [dctString, dctInteger, dctString, dctDouble]);
  Result.AddRow(['John', 30, 'New York', 75000.50]);
  Result.AddRow(['Alice', 25, 'Boston', 65000.75]);
  Result.AddRow(['Bob', 35, 'Chicago', 85000.25]);
  Result.AddRow(['Carol', Null, 'Denver', 70000.00]);
end;

function TDuckDBDataFrameTest.CreateNumericFrame: TDuckFrame;
begin
  Result := TDuckFrame.CreateBlank(['A', 'B', 'C'],
                                   [dctDouble, dctDouble, dctDouble]);

  // Create data with known values for testing
  // Column B will have values: 2, 4, 8, 11
  // Mean = (2 + 4 + 8 + 11) / 4 = 6.25
  // Median = (4 + 8) / 2 = 6.0
  Result.AddRow([1.0, 2.0, 3.0]);
  Result.AddRow([4.0, 4.0, 6.0]);
  Result.AddRow([7.0, 8.0, 9.0]);
  Result.AddRow([10.0, 11.0, 12.0]);
end;

procedure TDuckDBDataFrameTest.CreateSampleDatabase;
var
  DB: TDuckDBConnection;
begin
  FDBPath := FTempDir + 'test.db';
  DB := TDuckDBConnection.Create(FDBPath);
  try
    // Create sample table
    DB.ExecuteSQL(
      'CREATE TABLE sample_data (' + '  Name VARCHAR,' + '  Age INTEGER,' +
      '  City VARCHAR,' + '  Salary DOUBLE' + ')'
      );

    // Insert sample data
    DB.ExecuteSQL(
      'INSERT INTO sample_data VALUES ' +
      '(''John'', 30, ''New York'', 75000.50),' +
      '(''Alice'', 25, ''Boston'', 65000.75),' +
      '(''Bob'', 35, ''Chicago'', 85000.25),' +
      '(''Carol'', NULL, ''Denver'', 70000.00)'
      );
  finally
    DB.Free;
  end;
end;

procedure TDuckDBDataFrameTest.AssertFrameEquals(Expected, Actual: TDuckFrame;
  const Msg: string = '');
var
  Col, Row: integer;
begin
  AssertEquals(Msg + ' Column count mismatch',
    Expected.ColumnCount, Actual.ColumnCount);
  AssertEquals(Msg + ' Row count mismatch',
    Expected.RowCount, Actual.RowCount);

  for Col := 0 to Expected.ColumnCount - 1 do
  begin
    AssertEquals(Msg + Format(' Column %d name mismatch', [Col]),
      Expected.Columns[Col].Name, Actual.Columns[Col].Name);
    AssertEquals(Msg + Format(' Column %d type mismatch', [Col]),
      Ord(Expected.Columns[Col].DataType), Ord(Actual.Columns[Col].DataType));

    for Row := 0 to Expected.RowCount - 1 do
    begin
      if VarIsNull(Expected.Values[Row, Col]) then
        AssertTrue(Msg + Format(' Value mismatch at [%d,%d]', [Row, Col]),
          VarIsNull(Actual.Values[Row, Col]))
      else
        AssertEquals(Msg + Format(' Value mismatch at [%d,%d]', [Row, Col]),
          VarToStr(Expected.Values[Row, Col]), VarToStr(Actual.Values[Row, Col]));
    end;
  end;
end;

{ Constructor Tests }

procedure TDuckDBDataFrameTest.TestCreateBlank;
var
  Frame: TDuckFrame;
begin
  Frame := TDuckFrame.CreateBlank(['Col1', 'Col2', 'Col3'],
    [dctString, dctInteger, dctDouble]);
  try
    AssertEquals('Column count', 3, Frame.ColumnCount);
    AssertEquals('Row count', 0, Frame.RowCount);
    AssertEquals('First column name', 'Col1', Frame.Columns[0].Name);
    AssertEquals('First column type', Ord(dctString), Ord(Frame.Columns[0].DataType));
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestCreateBlankMismatchedArrays;
begin
  try
    TDuckFrame.CreateBlank(
      ['Col1', 'Col2'],
      [dctString]
      );
    Fail('Should raise exception for mismatched arrays');
  except
    on E: EDuckDBError do
      ; // Expected exception
  end;
end;

{ CSV Operations Tests }

procedure TDuckDBDataFrameTest.CreateSampleCSV(const FileName: string;
  const HasHeaders: boolean);
begin
  with TStringList.Create do
  try
    if HasHeaders then
      Add('Name,Age,City,Salary');
    Add('John,30,New York,75000.50');
    Add('Alice,25,Boston,65000.75');
    Add('Bob,35,Chicago,85000.25');
    Add('Carol,,Denver,70000.00');
    SaveToFile(FTempDir + FileName);
  finally
    Free;
  end;
end;

procedure TDuckDBDataFrameTest.CreateSampleCSVWithDelimiter(const FileName: string;
  const Delimiter: char);
var
  List: TStringList;
begin
  List := TStringList.Create;
  try
    // Add header
    List.Add('Name' + Delimiter + 'Age' + Delimiter + 'City' + Delimiter + 'Salary');
    // Add data
    List.Add('John' + Delimiter + '30' + Delimiter + 'New York' +
      Delimiter + '75000.50');
    List.Add('Alice' + Delimiter + '25' + Delimiter + 'Boston' + Delimiter + '65000.75');
    List.Add('Bob' + Delimiter + '35' + Delimiter + 'Chicago' + Delimiter + '85000.25');
    List.SaveToFile(FileName);
  finally
    List.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestCreateFromCSVWithHeaders;
var
  Frame: TDuckFrame;
begin
  CreateSampleCSV('test.csv', True);
  Frame := TDuckFrame.CreateFromCSV(FTempDir + 'test.csv');
  try
    AssertEquals('Column count', 4, Frame.ColumnCount);
    AssertEquals('Row count', 4, Frame.RowCount);
    AssertEquals('Column name', 'Name', Frame.Columns[0].Name);
    AssertEquals('First value', 'John', Frame.ValuesByName[0, 'Name']);
    AssertEquals('Numeric value', 30, Frame.ValuesByName[0, 'Age']);
    AssertTrue('Null value', VarIsNull(Frame.ValuesByName[3, 'Age']));
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestCreateFromCSVWithoutHeaders;
var
  Frame: TDuckFrame;
begin
  CreateSampleCSV('test.csv', False);
  Frame := TDuckFrame.CreateFromCSV(FTempDir + 'test.csv', False);
  try
    AssertEquals('Column count', 4, Frame.ColumnCount);
    AssertEquals('Row count', 4, Frame.RowCount);
    AssertEquals('Column name', 'column0', Frame.Columns[0].Name);
    AssertEquals('First value', 'John', Frame.Values[0, 0]);
  finally
    Frame.Free;
  end;
end;


procedure TDuckDBDataFrameTest.TestCreateFromCSVWithDifferentDelimiter;
var
  Frame: TDuckFrame;
  FilePath: string;
begin
  FilePath := FTempDir + 'test.csv';
  CreateSampleCSVWithDelimiter(FilePath, ';');  // Create CSV with semicolon delimiter

  Frame := TDuckFrame.CreateFromCSV(FilePath, True, ';');
  // Load with semicolon delimiter
  try
    AssertEquals('Column count', 4, Frame.ColumnCount);
    AssertEquals('Row count', 3, Frame.RowCount);
    AssertEquals('First value', string('John'), string(Frame.ValuesByName[0, 'Name']));
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestCreateFromDuckDB;
var
  Frame: TDuckFrame;
begin
  CreateSampleDatabase;  // Create the database and table first

  Frame := TDuckFrame.CreateFromDuckDB(FDBPath, 'sample_data');
  try
    AssertEquals('Column count', 4, Frame.ColumnCount);
    AssertEquals('Row count', 4, Frame.RowCount);

    // Test some values
    AssertEquals('First row name', string('John'),
      string(Frame.ValuesByName[0, 'Name']));
    AssertEquals('Second row age', integer(25), integer(Frame.ValuesByName[1, 'Age']));
    AssertEquals('Third row city', string('Chicago'),
      string(Frame.ValuesByName[2, 'City']));
    AssertEquals('Fourth row salary', double(70000.00),
      double(Frame.ValuesByName[3, 'Salary']));
  finally
    Frame.Free;
  end;
end;

{ Data Manipulation Tests }

procedure TDuckDBDataFrameTest.TestUnion;
var
  Frame1, Frame2, UnionFrame: TDuckFrame;
begin
  Frame1 := CreateSampleFrame;
  Frame2 := CreateSampleFrame;
  try
    // Add different row to Frame2
    Frame2.AddRow(['David', 40, 'Miami', 90000.00]);

    UnionFrame := Frame1.Union(Frame2);
    try
      AssertEquals('Column count', Frame1.ColumnCount, UnionFrame.ColumnCount);
      AssertEquals('Row count should exclude duplicates', 5, UnionFrame.RowCount);

      // Check if new row exists
      AssertTrue('New row should exist',
        UnionFrame.ValuesByName[UnionFrame.RowCount - 1, 'Name'] = 'David');
    finally
      UnionFrame.Free;
    end;
  finally
    Frame1.Free;
    Frame2.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestUnionAll;
var
  Frame1, Frame2, UnionFrame: TDuckFrame;
begin
  Frame1 := CreateSampleFrame;
  Frame2 := CreateSampleFrame;
  try
    UnionFrame := Frame1.UnionAll(Frame2);
    try
      AssertEquals('Column count', Frame1.ColumnCount, UnionFrame.ColumnCount);
      AssertEquals('Row count should include duplicates',
        Frame1.RowCount + Frame2.RowCount, UnionFrame.RowCount);
    finally
      UnionFrame.Free;
    end;
  finally
    Frame1.Free;
    Frame2.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestUnionWithDifferentColumns;
var
  Frame1, Frame2, UnionFrame: TDuckFrame;
begin
  Frame1 := TDuckFrame.CreateBlank(['A', 'B'], [dctInteger, dctString]);
  Frame2 := TDuckFrame.CreateBlank(['B', 'C'], [dctString, dctDouble]);
  try
    Frame1.AddRow([1, 'Test']);
    Frame2.AddRow(['Test', 2.5]);

    // Test umCommon mode
    UnionFrame := Frame1.Union(Frame2, umCommon);
    try
      AssertEquals('Common columns only', 1, UnionFrame.ColumnCount);
      AssertEquals('Common column name', 'B', UnionFrame.Columns[0].Name);
    finally
      UnionFrame.Free;
    end;

    // Test umAll mode
    UnionFrame := Frame1.Union(Frame2, umAll);
    try
      AssertEquals('All columns', 3, UnionFrame.ColumnCount);
      AssertTrue('Should have null values', VarIsNull(UnionFrame.ValuesByName[1, 'A']));
    finally
      UnionFrame.Free;
    end;
  finally
    Frame1.Free;
    Frame2.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestDistinct;
var
  Frame, DistinctFrame: TDuckFrame;
begin
  Frame := CreateSampleFrame;
  try
    // Add duplicate row
    Frame.AddRow(['John', 30, 'New York', 75000.50]);

    DistinctFrame := Frame.Distinct;
    try
      AssertEquals('Should remove duplicates', 4, DistinctFrame.RowCount);
    finally
      DistinctFrame.Free;
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestDropNA;
var
  Frame, CleanFrame: TDuckFrame;
  i, j: integer;
begin
  Frame := CreateSampleFrame;  // Contains one null value
  try
    CleanFrame := Frame.DropNA;
    try
      AssertEquals('Should remove row with null', 3, CleanFrame.RowCount);

      // Verify no nulls remain
      for i := 0 to CleanFrame.RowCount - 1 do
        for j := 0 to CleanFrame.ColumnCount - 1 do
          AssertFalse('Should not contain nulls',
            VarIsNull(CleanFrame.Values[i, j]));
    finally
      CleanFrame.Free;
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestFillNA;
var
  Frame, FilledFrame: TDuckFrame;
  i: integer;
begin
  Frame := CreateSampleFrame;
  try
    // Verify we have at least one NULL value before filling
    AssertTrue('Should have NULL value before filling',
      VarIsNull(Frame.ValuesByName[3, 'Age']));

    FilledFrame := Frame.FillNA(0);  // Fill nulls with 0
    try
      // Check that no nulls remain in the Age column
      for i := 0 to FilledFrame.RowCount - 1 do
        AssertFalse('Row ' + IntToStr(i) + ' should not be NULL',
          VarIsNull(FilledFrame.ValuesByName[i, 'Age']));

      // Original frame should still have nulls
      AssertTrue('Original frame should still have NULL',
        VarIsNull(Frame.ValuesByName[3, 'Age']));
    finally
      FilledFrame.Free;
    end;
  finally
    Frame.Free;
  end;
end;

{ Statistical Operations Tests }

procedure TDuckDBDataFrameTest.TestDescribe;
var
  Frame, Stats: TDuckFrame;
begin
  Frame := CreateNumericFrame;
  try
    Frame.Describe;
    // Just test that Print doesn't raise an exception
    Frame.Print;
    // Note: We can't easily test the actual output since it goes to console
    // Could potentially redirect stdout ...
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestNullCount;
var
  Frame, NullCounts: TDuckFrame;
begin
  Frame := CreateSampleFrame;  // Contains one null in Age column
  try
    NullCounts := Frame.NullCount;
    try
      AssertEquals('Should have one row', 1, NullCounts.RowCount);
      AssertEquals('Null count in Age column', 1,
        NullCounts.ValuesByName[0, 'Age']);
      AssertEquals('Null count in Name column', 0,
        NullCounts.ValuesByName[0, 'Name']);
    finally
      NullCounts.Free;
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestCorrPearson;
var
  Frame, Correlation: TDuckFrame;
  i, j: integer;
begin
  Frame := CreateNumericFrame;
  try
    Correlation := Frame.CorrPearson;
    try
      AssertEquals('Should be square matrix',
        Frame.ColumnCount, Correlation.ColumnCount);
      AssertEquals('Should be square matrix',
        Frame.ColumnCount, Correlation.RowCount);

      // Diagonal should be 1.0
      for i := 0 to Correlation.ColumnCount - 1 do
        AssertEquals('Diagonal should be 1.0', 1.0,
          Correlation.Values[i, i]);
    finally
      Correlation.Free;
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestCorrSpearman;
var
  Frame, Correlation: TDuckFrame;
  i: integer;
begin
  Frame := CreateNumericFrame;
  try
    Correlation := Frame.CorrSpearman;
    try
      AssertEquals('Should be square matrix',
        Frame.ColumnCount, Correlation.ColumnCount);
      AssertEquals('Should be square matrix',
        Frame.ColumnCount, Correlation.RowCount);

      // Diagonal should be 1.0
      for i := 0 to Correlation.ColumnCount - 1 do
        AssertEquals('Diagonal should be 1.0', 1.0,
          Correlation.Values[i, i]);
    finally
      Correlation.Free;
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestCalculateColumnStats;
var
  Frame: TDuckFrame;
  Stats: TColumnStats;
begin
  Frame := CreateNumericFrame;
  try
    Stats := Frame.CalculateColumnStats(Frame.GetColumnByName('B'));

    AssertEquals('Count', 4, Stats.Count);
    AssertEquals('Mean', 6.25, Stats.Mean);
    AssertEquals('Min', 2.0, Stats.Min);
    AssertEquals('Max', 11.0, Stats.Max);
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestCalculatePercentile;
var
  Frame: TDuckFrame;
  ColumnData: array of double;
  i: integer;
  Column: TDuckDBColumn;
begin
  Frame := CreateNumericFrame;
  try
    // Get column B data and convert to array of Double
    Column := Frame.GetColumnByName('B');
    SetLength(ColumnData, Frame.RowCount);
    for i := 0 to Frame.RowCount - 1 do
    begin
      if not VarIsNull(Column.Data[i]) then
        ColumnData[i] := double(Column.Data[i]);
    end;

    // Test percentiles
    AssertEquals('Median of column B', 6.0,
      Frame.CalculatePercentile(ColumnData, 0.5));

    AssertEquals('25th percentile of column B', 3.5,
      Frame.CalculatePercentile(ColumnData, 0.25));

    AssertEquals('75th percentile of column B', 8.75,
      Frame.CalculatePercentile(ColumnData, 0.75));
  finally
    Frame.Free;
  end;
end;

{ Error Handling Tests }

procedure TDuckDBDataFrameTest.TestInvalidColumnAccess;
var
  Frame: TDuckFrame;
begin
  Frame := CreateSampleFrame;
  try
    try
      Frame.ValuesByName[0, 'NonExistentColumn'];
      Fail('Should raise exception for invalid column name');
    except
      on E: EDuckDBError do
        AssertTrue('Should contain column name in error',
          Pos('NonExistentColumn', E.Message) > 0);
    end;

    try
      Frame.Values[0, Frame.ColumnCount];
      Fail('Should raise exception for invalid column index');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestInvalidRowAccess;
var
  Frame: TDuckFrame;
begin
  Frame := CreateSampleFrame;
  try
    try
      Frame.Values[Frame.RowCount, 0];
      Fail('Should raise exception for invalid row index');
    except
      on E: EDuckDBError do
        AssertTrue('Should contain row index in error',
          Pos('row index', LowerCase(E.Message)) > 0);
    end;

    try
      Frame.Values[-1, 0];
      Fail('Should raise exception for negative row index');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;
  finally
    Frame.Free;
  end;
end;

{ Type Conversion Tests }

procedure TDuckDBDataFrameTest.TestTryConvertValue;
var
  Frame: TDuckFrame;
  Value: variant;
begin
  Frame := CreateSampleFrame;
  try
    // Test valid conversions
    Value := Frame.TryConvertValue('123', dctString, dctInteger);
    AssertEquals('Valid integer conversion', 123, integer(Value));

    Value := Frame.TryConvertValue('123.45', dctString, dctDouble);
    AssertEquals('Valid double conversion', 123.45, double(Value));

    // Test invalid conversion gives null
    Value := Frame.TryConvertValue('abc', dctString, dctInteger);
    AssertTrue('Invalid conversion string to int', VarIsNull(Value));

    Value := Frame.TryConvertValue('xyz', dctString, dctDouble);
    AssertTrue('Invalid conversion string to double should be NULL', VarIsNull(Value));
  finally
    Frame.Free;
  end;
end;

{ Visualization Tests }
procedure TDuckDBDataFrameTest.TestPlotHistogram;
var
  Frame: TDuckFrame;
begin
  // Create a frame with numeric data for histogram
  Frame := CreateSampleFrame;
  try
    // Just test that Print doesn't raise an exception
    Frame.PlotHistogram('Salary');
    // Note: We can't easily test the actual output since it goes to console
    // We could potentially redirect stdout ...
  finally
    Frame.Free;
  end;
end;

{ Print and Info Tests }

procedure TDuckDBDataFrameTest.TestPrint;
var
  Frame: TDuckFrame;
begin
  Frame := CreateSampleFrame;
  try
    // Just test that Print doesn't raise an exception
    Frame.Print;
    // Note: We can't easily test the actual output since it goes to console
    // Could potentially redirect stdout ...
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestInfo;
var
  Frame: TDuckFrame;
begin
  Frame := CreateSampleFrame;
  try
    // Just test that Print doesn't raise an exception
    Frame.Info;
    // Note: We can't easily test the actual output since it goes to console
    // Could potentially redirect stdout ...
  finally
    Frame.Free;
  end;
end;

{ Additional Helper Tests }

procedure TDuckDBDataFrameTest.TestClear;
var
  Frame: TDuckFrame;
  OriginalColumnCount: integer;
begin
  Frame := CreateSampleFrame;
  try
    OriginalColumnCount := Frame.ColumnCount;
    AssertTrue('Should have rows before clear', Frame.RowCount > 0);

    Frame.Clear;

    AssertEquals('Column count should remain', OriginalColumnCount, Frame.ColumnCount);
    AssertEquals('Row count should be zero', 0, Frame.RowCount);

    // Verify column names are preserved
    AssertEquals('First column name preserved', 'Name', Frame.GetColumnNames[0]);
    AssertEquals('Second column name preserved', 'Age', Frame.GetColumnNames[1]);
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestAddColumn;
var
  Frame: TDuckFrame;
  i: integer;
begin
  Frame := CreateSampleFrame;
  try
    Frame.AddColumn('NewColumn', dctString);
    AssertEquals('Column count increased', 5, Frame.ColumnCount);
    AssertEquals('New column name', 'NewColumn', Frame.Columns[4].Name);
    AssertEquals('New column type', Ord(dctString), Ord(Frame.Columns[4].DataType));

    // Verify all values in new column are null
    for i := 0 to Frame.RowCount - 1 do
      AssertTrue('New column values should be null',
        VarIsNull(Frame.ValuesByName[i, 'NewColumn']));

    // Test adding duplicate column name
    try
      Frame.AddColumn('NewColumn', dctString);
      Fail('Should raise exception for duplicate column name');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestAddRow;
var
  Frame: TDuckFrame;
  NewRow: array of variant;
begin
  Frame := CreateSampleFrame;
  try
    SetLength(NewRow, Frame.ColumnCount);
    NewRow[0] := 'David';
    NewRow[1] := 40;
    NewRow[2] := 'Miami';
    NewRow[3] := 90000.00;

    Frame.AddRow(NewRow);
    AssertEquals('Row count increased', 5, Frame.RowCount);
    AssertEquals('New row value', 'David', Frame.ValuesByName[4, 'Name']);

    // Test adding row with wrong number of values
    SetLength(NewRow, Frame.ColumnCount - 1);
    try
      Frame.AddRow(NewRow);
      Fail('Should raise exception for wrong number of values');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestSetValue;
var
  Frame: TDuckFrame;
  ExpectedSalary: double;
begin
  Frame := CreateSampleFrame;
  try
    ExpectedSalary := 95000.75;
    Frame.SetValue(2, 'Salary', ExpectedSalary);
    AssertEquals('Salary should be updated', ExpectedSalary, double(Frame.ValuesByName[2, 'Salary']));
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestGetColumnByName;
var
  Frame: TDuckFrame;
  Column: TDuckDBColumn;
begin
  Frame := CreateSampleFrame;
  try
    // Test getting valid column
    Column := Frame.GetColumnByName('Age');
    AssertEquals('Column name should match', 'Age', Column.Name);
    AssertEquals('Column type should match', Ord(dctInteger), Ord(Column.DataType));
    AssertEquals('Column data length should match frame row count',
      Frame.RowCount, Length(Column.Data));

    // Test getting non-existent column
    try
      Column := Frame.GetColumnByName('NonExistentColumn');
      Fail('Should raise exception for non-existent column');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestGetColumnNames;
var
  Frame: TDuckFrame;
  Names: TStringArray;
begin
  Frame := CreateSampleFrame;
  try
    Names := Frame.GetColumnNames;
    AssertEquals('Should have correct number of columns', 4, Length(Names));
    AssertEquals('First column name', 'Name', Names[0]);
    AssertEquals('Second column name', 'Age', Names[1]);
    AssertEquals('Third column name', 'City', Names[2]);
    AssertEquals('Fourth column name', 'Salary', Names[3]);
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestFindColumnIndex;
var
  Frame: TDuckFrame;
begin
  Frame := CreateSampleFrame;
  try
    AssertEquals('Should find Name column', 0, Frame.FindColumnIndex('Name'));
    AssertEquals('Should find Age column', 1, Frame.FindColumnIndex('Age'));
    AssertEquals('Should find City column', 2, Frame.FindColumnIndex('City'));
    AssertEquals('Should find Salary column', 3, Frame.FindColumnIndex('Salary'));
    AssertEquals('Should return -1 for non-existent column', -1,
      Frame.FindColumnIndex('NonExistentColumn'));
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestHead;
var
  Frame, HeadFrame: TDuckFrame;
  Value: variant;
begin
  Frame := CreateSampleFrame;
  try
    // Test default head (first 5 rows)
    HeadFrame := Frame.Head;
    try
      AssertEquals('Should have same number of columns', Frame.ColumnCount,
        HeadFrame.ColumnCount);
      AssertEquals('Should have all rows since sample has less than 5 rows',
        Frame.RowCount, HeadFrame.RowCount);

      Value := HeadFrame.ValuesByName[0, 'Name'];
      AssertEquals('First row name should match', string('John'), string(Value));
    finally
      HeadFrame.Free;
    end;

    // Test head with specific number of rows
    HeadFrame := Frame.Head(2);
    try
      AssertEquals('Should have same number of columns', Frame.ColumnCount,
        HeadFrame.ColumnCount);
      AssertEquals('Should have requested number of rows', 2, HeadFrame.RowCount);

      Value := HeadFrame.ValuesByName[0, 'Name'];
      AssertEquals('First row name should match', string('John'), string(Value));

      Value := HeadFrame.ValuesByName[1, 'Name'];
      AssertEquals('Second row name should match', string('Alice'), string(Value));
    finally
      HeadFrame.Free;
    end;

    // Test head with number larger than available rows
    HeadFrame := Frame.Head(10);
    try
      AssertEquals('Should have all available rows', Frame.RowCount, HeadFrame.RowCount);
    finally
      HeadFrame.Free;
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestValuesByName;
var
  Frame: TDuckFrame;
  Value: variant;
begin
  Frame := CreateSampleFrame;
  try
    // Test getting values by name
    Value := Frame.ValuesByName[0, 'Name'];
    AssertEquals('First row name', string('John'), string(Value));

    Value := Frame.ValuesByName[1, 'Age'];
    AssertEquals('Second row age', integer(25), integer(Value));

    Value := Frame.ValuesByName[2, 'City'];
    AssertEquals('Third row city', string('Chicago'), string(Value));

    Value := Frame.ValuesByName[3, 'Salary'];
    AssertEquals('Fourth row salary', double(70000.00), double(Value));

    // Test NULL value
    Value := Frame.ValuesByName[3, 'Age'];
    AssertTrue('Should be NULL', VarIsNull(Value));

    // Test invalid row index
    try
      Value := Frame.ValuesByName[-1, 'Name'];
      Fail('Should raise exception for invalid row index');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;

    // Test invalid column name
    try
      Value := Frame.ValuesByName[0, 'NonExistentColumn'];
      Fail('Should raise exception for invalid column name');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestValues;
var
  Frame: TDuckFrame;
  Value: variant;
begin
  Frame := CreateSampleFrame;
  try
    // Test getting values by indices
    Value := Frame.Values[0, 0];  // First row, first column (Name)
    AssertEquals('First row, first column', 'John', Value);

    Value := Frame.Values[1, 1];  // Second row, second column (Age)
    AssertEquals('Second row, second column', 25, Value);

    Value := Frame.Values[2, 2];  // Third row, third column (City)
    AssertEquals('Third row, third column', 'Chicago', Value);

    Value := Frame.Values[3, 3];  // Fourth row, fourth column (Salary)
    AssertEquals('Fourth row, fourth column', 70000.00, Value);

    // Test NULL value
    Value := Frame.Values[3, 1];  // Fourth row, second column (Age)
    AssertTrue('Should be NULL', VarIsNull(Value));

    // Test invalid row index
    try
      Value := Frame.Values[-1, 0];
      Fail('Should raise exception for invalid row index');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;

    // Test invalid column index
    try
      Value := Frame.Values[0, Frame.ColumnCount];
      Fail('Should raise exception for invalid column index');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestSaveToCSV;
var
  Frame: TDuckFrame;
  SavePath: string;
  LoadedFrame: TDuckFrame;
begin
  Frame := CreateSampleFrame;
  try
    SavePath := FTempDir + 'test_save.csv';

    // Test saving to CSV
    Frame.SaveToCSV(SavePath);
    AssertTrue('CSV file should exist', FileExists(SavePath));

    // Load the saved file and verify contents
    LoadedFrame := TDuckFrame.CreateFromCSV(SavePath, True);  // with headers
    try
      AssertEquals('Should have same number of columns', Frame.ColumnCount,
        LoadedFrame.ColumnCount);
      AssertEquals('Should have same number of rows', Frame.RowCount,
        LoadedFrame.RowCount);

      // Verify some values
      AssertEquals('First row name should match',
        string(Frame.ValuesByName[0, 'Name']),
        string(LoadedFrame.ValuesByName[0, 'Name']));
      AssertEquals('Second row age should match',
        integer(Frame.ValuesByName[1, 'Age']),
        integer(LoadedFrame.ValuesByName[1, 'Age']));
      AssertEquals('Third row city should match',
        string(Frame.ValuesByName[2, 'City']),
        string(LoadedFrame.ValuesByName[2, 'City']));
    finally
      LoadedFrame.Free;
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestInvalidCSVFile;
var
  Frame: TDuckFrame;
begin
  // Test loading non-existent file
  try
    Frame := TDuckFrame.CreateFromCSV('nonexistent.csv', True);
    Fail('Should raise exception for non-existent file');
  except
    on E: EDuckDBError do
      ; // Expected exception
  end;

  // Test loading invalid file path
  try
    Frame := TDuckFrame.CreateFromCSV('', True);
    Fail('Should raise exception for empty file path');
  except
    on E: EDuckDBError do
      ; // Expected exception
  end;

  // Test loading directory instead of file
  try
    Frame := TDuckFrame.CreateFromCSV(FTempDir, True);
    Fail('Should raise exception when path is a directory');
  except
    on E: EDuckDBError do
      ; // Expected exception
  end;

  // Test saving to invalid path
  Frame := CreateSampleFrame;
  try
    try
      Frame.SaveToCSV('');
      Fail('Should raise exception for empty save path');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;

    try
      Frame.SaveToCSV('/invalid/path/file.csv');
      Fail('Should raise exception for invalid save path');
    except
      on E: EDuckDBError do
        ; // Expected exception
    end;
  finally
    Frame.Free;
  end;
end;

procedure TDuckDBDataFrameTest.TestDateTimeArrays;
var
  Frame: TDuckFrame;
  Column: TDuckDBColumn;
  DateTimes: specialize TArray<TDateTime>;
  Dates: specialize TArray<TDate>;
  Times: specialize TArray<TTime>;
begin
  // Create frame with date/time data
  Frame := TDuckFrame.CreateBlank(['Timestamp', 'Date', 'Time'],
    [dctTimestamp, dctDate, dctTime]);
    
  Frame.AddRow([
    EncodeDateTime(2023, 11, 25, 10, 30, 0, 0),  // 10:30 AM, Nov 25, 2023
    EncodeDate(2023, 11, 25),                     // Nov 25, 2023
    EncodeTime(10, 30, 0, 0)                      // 10:30 AM
  ]);

  try
    // Test timestamp column
    Column := Frame.GetColumnByName('Timestamp');
    DateTimes := Column.AsDateTimeArray;
    AssertEquals('Year of timestamp', 2023,
      YearOf(DateTimes[0]));
    AssertEquals('Hour of timestamp', 10,
      HourOf(DateTimes[0]));
      
    // Test date column
    Column := Frame.GetColumnByName('Date');
    Dates := Column.AsDateArray;
    AssertEquals('Year of date', 2023, YearOf(Dates[0]));
    AssertEquals('Month of date', 11, MonthOf(Dates[0]));
      
    // Test time column
    Column := Frame.GetColumnByName('Time');
    Times := Column.AsTimeArray;
    AssertEquals('Hour of time', 10, HourOf(Times[0]));
    AssertEquals('Minute of time', 30, MinuteOf(Times[0]));

  finally
    Frame.Free;
  end;
end;


procedure TDuckDBDataFrameTest.TestTypeConversion;
begin
  // Test DuckDBTypeToString
  AssertEquals('Integer type string', 'INTEGER',
    DuckDBTypeToString(dctInteger));
  AssertEquals('Time type string', 'TIME',
    DuckDBTypeToString(dctTime));

  // Test StringToDuckDBType - Fix: Compare ordinal values
  AssertEquals('INTEGER to type', Ord(dctInteger),
    Ord(StringToDuckDBType('INTEGER')));
  AssertEquals('INT to type', Ord(dctInteger),
    Ord(StringToDuckDBType('INT')));
  AssertEquals('TIME to type', Ord(dctTime),
    Ord(StringToDuckDBType('TIME')));

  // Test round-trip conversion - Fix: Compare ordinal values
  AssertEquals('Round-trip INTEGER', Ord(dctInteger),
    Ord(StringToDuckDBType(DuckDBTypeToString(dctInteger))));
  AssertEquals('Round-trip TIME', Ord(dctTime),
    Ord(StringToDuckDBType(DuckDBTypeToString(dctTime))));
end;


initialization

  RegisterTest(TDuckDBDataFrameTest);
end.
