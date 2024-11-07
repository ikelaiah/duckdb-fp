program UnionDuckFrames;

{$mode objfpc}{$H+}{$J-}

uses
  Classes, SysUtils, DuckDB.DataFrame, DuckDB.SampleData, DuckDB.Wrapper;

procedure DemonstrateDifferentStructures;
var
  DB: TDuckDBConnection;
  DF1, DF2, Combined, Distinct: TDuckFrame;
begin
  WriteLn('Demonstrating Union with Different Structures:');
  WriteLn('-------------------------------------------');
  
  DB := TDuckDBConnection.Create(':memory:');
  try
    // Create DataFrames with different structures
    DF1 := DB.Query('SELECT 1 as id, ''A'' as name, 25 as age');
    DF2 := DB.Query('SELECT 2 as id, ''B'' as name, ''HR'' as department');
    
    try
      WriteLn('DataFrame 1:');
      DF1.Print;
      WriteLn;
      
      WriteLn('DataFrame 2:');
      DF2.Print;
      WriteLn;
      
      // Try different union modes
      WriteLn('Union with umCommon (only shared columns):');
      Combined := DF1.UnionAll(DF2, umCommon);
      try
        Combined.Print;
        WriteLn('After removing duplicates:');
        Distinct := Combined.Distinct;
        try
          Distinct.Print;
        finally
          Distinct.Free;
        end;
      finally
        Combined.Free;
      end;
      WriteLn;
      
      WriteLn('UnionAll with umAll (all columns, NULL for missing):');
      Combined := DF1.UnionAll(DF2, umAll);
      try
        Combined.Print;
      finally
        Combined.Free;
      end;
    finally
      DF1.Free;
      DF2.Free;
    end;
  finally
    DB.Free;
  end;
end;

procedure DemonstrateDuplicateHandling;
var
  DB: TDuckDBConnection;
  DF1, DF2, Combined, Distinct: TDuckFrame;
begin
  WriteLn('Demonstrating Duplicate Handling:');
  WriteLn('------------------------------');
  
  DB := TDuckDBConnection.Create(':memory:');
  try
    // Create DataFrames with some duplicate data
    DF1 := DB.Query('SELECT * FROM (VALUES (1, ''A''), (2, ''B''), (1, ''A'')) AS t(id, name)');
    DF2 := DB.Query('SELECT * FROM (VALUES (3, ''C''), (4, ''D'')) AS t(id, name)');
    
    try
      WriteLn('DataFrame 1 (with duplicates):');
      DF1.Print;
      WriteLn;
      
      WriteLn('DataFrame 2:');
      DF2.Print;
      WriteLn;
      
      WriteLn('UnionAll (keeps duplicates):');
      Combined := DF1.UnionAll(DF2);
      try
        Combined.Print;
        WriteLn('After removing duplicates:');
        Distinct := Combined.Distinct;
        try
          Distinct.Print;
        finally
          Distinct.Free;
        end;
      finally
        Combined.Free;
      end;
      WriteLn;
      
      WriteLn('Distinct on DataFrame 1 (removes duplicates):');
      Combined := DF1.Distinct;
      try
        Combined.Print;
      finally
        Combined.Free;
      end;
    finally
      DF1.Free;
      DF2.Free;
    end;
  finally
    DB.Free;
  end;
end;

begin
  DemonstrateDifferentStructures;
  WriteLn;
  DemonstrateDuplicateHandling;
  
  WriteLn('Press Enter to quit...');
  ReadLn;
end.
