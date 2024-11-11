program DuckDB.FP.Tests;

{$mode objfpc}{$H+}

uses
  Classes, consoletestrunner, DuckDB.FreePascal.Cases;

type

  { TMyTestRunner }

  TMyTestRunner = class(TTestRunner)
  protected
  // override the protected methods of TTestRunner to customize its behavior
  end;

var
  Application: TMyTestRunner;

begin
  Application := TMyTestRunner.Create(nil);
  Application.Initialize;
  Application.Title := 'DuckDB for FreePascal Tests';
  Application.Run;
  Application.Free;
end.
