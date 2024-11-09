unit DuckDB.SampleData;

{$mode objfpc}{$H+}{$J-}

interface

uses
  SysUtils, Classes, DuckDB.DataFrame, DuckDB.Wrapper;

type
  { Available sample datasets }
  TSampleDataset = (
    sdMtcars,    // Motor Trend Car Road Tests
    sdIris,      // Iris Flower Dataset
    sdTitanic,   // Titanic Passenger Data
    sdDiamonds,  // Diamond Prices and Characteristics
    sdGapminder, // Gapminder World Data
    sdMpg,       // Fuel Economy Data
    sdNYCFlights // NYC Flight Data
  );

  { TDuckDBSampleData }
  TDuckDBSampleData = class
  private
    class var FConnection: TDuckDBConnection;
    class function GetConnection: TDuckDBConnection; static;
    class function GetSampleDataPath: string; static;
  public
    class function LoadData(Dataset: TSampleDataset): TDuckFrame;
    class function GetDataInfo(Dataset: TSampleDataset): string;
    class destructor Destroy;
  end;

implementation

class function TDuckDBSampleData.GetConnection: TDuckDBConnection;
begin
  if FConnection = nil then
    FConnection := TDuckDBConnection.Create(':memory:');
  Result := FConnection;
end;

class function TDuckDBSampleData.GetSampleDataPath: string;
var
  BasePath: string;
begin
  // Start from executable path
  BasePath := ExtractFilePath(ParamStr(0));
  
  // If we're in examples/LoadSampleData, go up two levels
  if Pos('examples', LowerCase(BasePath)) > 0 then
    Result := IncludeTrailingPathDelimiter(ExtractFilePath(ExcludeTrailingPathDelimiter(
      ExtractFilePath(ExcludeTrailingPathDelimiter(BasePath))))) + 'sample_data'
  else
    // Otherwise assume we're in the project root
    Result := IncludeTrailingPathDelimiter(BasePath) + 'sample_data';
end;

class function TDuckDBSampleData.LoadData(Dataset: TSampleDataset): TDuckFrame;
var
  Query: string;
  DataDir: string;
begin
  Result := nil;
  Query := '';
  
  DataDir := GetSampleDataPath;
  
  if not DirectoryExists(DataDir) then
    raise EDuckDBError.CreateFmt('Sample data directory not found: %s', [DataDir]);
  
  case Dataset of
    sdMtcars:
      Query := Format('SELECT * FROM read_csv_auto(''%s'')', [
        StringReplace(DataDir + PathDelim + 'mtcars.csv', '''', '''''', [rfReplaceAll])
      ]);
    sdIris:
      Query := Format('SELECT * FROM read_csv_auto(''%s'')', [
        StringReplace(DataDir + PathDelim + 'iris.csv', '''', '''''', [rfReplaceAll])
      ]);
    sdTitanic:
      Query := Format('SELECT * FROM read_csv_auto(''%s'')', [
        StringReplace(DataDir + PathDelim + 'titanic.csv', '''', '''''', [rfReplaceAll])
      ]);
    sdDiamonds:
      Query := Format('SELECT * FROM read_csv_auto(''%s'')', [
        StringReplace(DataDir + PathDelim + 'diamonds.csv', '''', '''''', [rfReplaceAll])
      ]);
    sdGapminder:
      Query := Format('SELECT * FROM read_csv_auto(''%s'')', [
        StringReplace(DataDir + PathDelim + 'gapminder.csv', '''', '''''', [rfReplaceAll])
      ]);
    sdMpg:
      Query := Format('SELECT * FROM read_csv_auto(''%s'')', [
        StringReplace(DataDir + PathDelim + 'mpg.csv', '''', '''''', [rfReplaceAll])
      ]);
    sdNYCFlights:
      Query := Format('SELECT * FROM read_csv_auto(''%s'')', [
        StringReplace(DataDir + PathDelim + 'nycflights.csv', '''', '''''', [rfReplaceAll])
      ]);
  end;
  
  Result := GetConnection.Query(Query);
end;

class function TDuckDBSampleData.GetDataInfo(Dataset: TSampleDataset): string;
begin
  Result := '';
  case Dataset of
    sdMtcars:
      Result := 'Motor Trend Car Road Tests' + LineEnding +
                'A dataset of 1974 Motor Trend US magazine' + LineEnding +
                '32 observations on 11 variables including:' + LineEnding +
                '- mpg: Miles/(US) gallon' + LineEnding +
                '- cyl: Number of cylinders' + LineEnding +
                '- disp: Displacement (cu.in.)' + LineEnding +
                '- hp: Gross horsepower' + LineEnding +
                '- wt: Weight (1000 lbs)';

    sdIris:
      Result := 'Iris Flower Dataset' + LineEnding +
                'Famous dataset by R.A. Fisher' + LineEnding +
                '150 observations on 5 variables:' + LineEnding +
                '- sepal_length: Sepal length in cm' + LineEnding +
                '- sepal_width: Sepal width in cm' + LineEnding +
                '- petal_length: Petal length in cm' + LineEnding +
                '- petal_width: Petal width in cm' + LineEnding +
                '- species: Species of iris';

    sdTitanic:
      Result := 'Titanic Passenger Data' + LineEnding +
                'Survival data for Titanic passengers' + LineEnding +
                '891 observations on 12 variables including:' + LineEnding +
                '- survived: Survival (0 = No, 1 = Yes)' + LineEnding +
                '- pclass: Passenger Class (1 = 1st, 2 = 2nd, 3 = 3rd)' + LineEnding +
                '- name: Passenger Name' + LineEnding +
                '- sex: Gender' + LineEnding +
                '- age: Age in years' + LineEnding +
                '- fare: Passenger fare';

    sdDiamonds:
      Result := 'Diamond Prices and Characteristics' + LineEnding +
                'A dataset of diamond prices and their characteristics' + LineEnding +
                '53,940 observations on 10 variables including:' + LineEnding +
                '- carat: Weight of diamond' + LineEnding +
                '- cut: Quality of cut' + LineEnding +
                '- color: Diamond color' + LineEnding +
                '- clarity: Clarity rating' + LineEnding +
                '- price: Price in US dollars';

    sdGapminder:
      Result := 'Gapminder World Data' + LineEnding +
                'Excerpt of Gapminder World Dataset' + LineEnding +
                'Data about countries through the years including:' + LineEnding +
                '- country: Country name' + LineEnding +
                '- continent: Continent' + LineEnding +
                '- year: Year of observation' + LineEnding +
                '- lifeExp: Life expectancy' + LineEnding +
                '- pop: Population' + LineEnding +
                '- gdpPercap: GDP per capita';

    sdMpg:
      Result := 'Fuel Economy Data' + LineEnding +
                'Fuel economy data from 1999 to 2008 for 38 popular models' + LineEnding +
                'Variables include:' + LineEnding +
                '- manufacturer: Manufacturer name' + LineEnding +
                '- model: Model name' + LineEnding +
                '- displ: Engine displacement in liters' + LineEnding +
                '- year: Year of manufacture' + LineEnding +
                '- cyl: Number of cylinders' + LineEnding +
                '- trans: Type of transmission' + LineEnding +
                '- hwy: Highway miles per gallon';

    sdNYCFlights:
      Result := 'NYC Flight Data' + LineEnding +
                'Flight data for all flights departing NYC in 2013' + LineEnding +
                'Variables include:' + LineEnding +
                '- year, month, day: Date of flight' + LineEnding +
                '- carrier: Carrier code' + LineEnding +
                '- flight: Flight number' + LineEnding +
                '- origin, dest: Origin and destination airports' + LineEnding +
                '- air_time: Flight time in minutes' + LineEnding +
                '- distance: Flight distance in miles' + LineEnding +
                '- hour, minute: Scheduled departure time';
  end;
end;

class destructor TDuckDBSampleData.Destroy;
begin
  FConnection.Free;
  inherited;
end;

end. 
