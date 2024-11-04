program DemoWrapper03;

{$mode objfpc}{$H+}

uses
  SysUtils,
  DuckDB.Wrapper,
  DuckDB.DataFrame;

var
  DB: TDuckDBConnection;
  DF, NullCounts: TDuckFrame;
begin
  try
    DB := TDuckDBConnection.Create;
    try
      DB.Open;

      DB.ExecuteSQL('CREATE TABLE employees (id INTEGER, name VARCHAR, age INTEGER, salary DOUBLE)');
      DB.ExecuteSQL('INSERT INTO employees VALUES (1, ''John'', 25, 50000)');
      DB.ExecuteSQL('INSERT INTO employees VALUES (2, ''Jane'', 30, NULL)');
      DB.ExecuteSQL('INSERT INTO employees VALUES (3, ''Bob'', NULL, 75000)');
      DB.ExecuteSQL('INSERT INTO employees VALUES (4, ''Alice'', 45, 80000)');

      DF := DB.Query('SELECT * FROM employees');
      try
        WriteLn('Statistical Summary:');
        WriteLn('-------------------');
        DF.Describe;
        WriteLn;

        WriteLn('Null Value Counts:');
        WriteLn('----------------');
        NullCounts := DF.NullCount;
        try
          NullCounts.Print;
        finally
          NullCounts.Free;
        end;
        WriteLn;

        WriteLn('DataFrame Info:');
        WriteLn('--------------');
        DF.Info;
      finally
        DF.Free;
      end;

    finally
      DB.Free;
    end;

  except
    on E: Exception do
      WriteLn(E.ClassName, ': ', E.Message);
  end;

  WriteLn('Press Enter to exit...');
  ReadLn;
end.
