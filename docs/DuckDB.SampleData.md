# DuckDB.SampleData API Reference

The `DuckDB.SampleData` unit provides easy access to common datasets for testing and learning purposes.

## Types

### TSampleDataset
Enumeration of available sample datasets:
```pascal
TSampleDataset = (
  sdMtcars,    // Motor Trend Car Road Tests
  sdIris,      // Iris Flower Dataset
  sdTitanic,   // Titanic Passenger Data
  sdDiamonds,  // Diamond Prices and Characteristics
  sdGapminder, // Gapminder World Data
  sdMpg,       // Fuel Economy Data
  sdNYCFlights // NYC Flight Data
);
```

## TDuckDBSampleData Class

### Class Methods

#### LoadData
```pascal
class function LoadData(Dataset: TSampleDataset): TDuckFrame;
```
Loads a sample dataset into a DataFrame.

Parameters:
- `Dataset`: The dataset to load (TSampleDataset)

Returns:
- `TDuckFrame`: A new DataFrame containing the dataset

Example:
```pascal
var
  DF: TDuckFrame;
begin
  DF := TDuckDBSampleData.LoadData(sdMtcars);
  try
    DF.Print;
  finally
    DF.Free;
  end;
end;
```

#### GetDataInfo
```pascal
class function GetDataInfo(Dataset: TSampleDataset): string;
```
Returns detailed information about a dataset.

Parameters:
- `Dataset`: The dataset to get information about (TSampleDataset)

Returns:
- `string`: Multi-line description of the dataset including variables and their meanings

Example:
```pascal
WriteLn(TDuckDBSampleData.GetDataInfo(sdIris));
```

## Dataset Details

### MTCars (sdMtcars)
Motor Trend Car Road Tests dataset containing 32 observations on 11 variables:
- mpg: Miles/(US) gallon
- cyl: Number of cylinders
- disp: Displacement (cu.in.)
- hp: Gross horsepower
- wt: Weight (1000 lbs)
- And more...

### Iris (sdIris)
Fisher's Iris Flower Dataset with 150 observations on 5 variables:
- sepal_length: Sepal length in cm
- sepal_width: Sepal width in cm
- petal_length: Petal length in cm
- petal_width: Petal width in cm
- species: Species of iris

### Titanic (sdTitanic)
Titanic Passenger Data with 891 observations on 12 variables:
- survived: Survival (0 = No, 1 = Yes)
- pclass: Passenger Class (1 = 1st, 2 = 2nd, 3 = 3rd)
- name: Passenger Name
- sex: Gender
- age: Age in years
- fare: Passenger fare
- And more...

### Diamonds (sdDiamonds)
Diamond Prices Dataset with 53,940 observations on 10 variables:
- carat: Weight of diamond
- cut: Quality of cut
- color: Diamond color
- clarity: Clarity rating
- price: Price in US dollars
- And more...

### Gapminder (sdGapminder)
World Development Indicators including:
- country: Country name
- continent: Continent
- year: Year of observation
- lifeExp: Life expectancy
- pop: Population
- gdpPercap: GDP per capita

### MPG (sdMpg)
Fuel Economy Data from 1999 to 2008 including:
- manufacturer: Manufacturer name
- model: Model name
- displ: Engine displacement in liters
- year: Year of manufacture
- cyl: Number of cylinders
- trans: Type of transmission
- hwy: Highway miles per gallon
- And more...

### NYC Flights (sdNYCFlights)
2013 Flight Data from NYC including:
- year, month, day: Date of flight
- carrier: Carrier code
- flight: Flight number
- origin, dest: Origin and destination airports
- air_time: Flight time in minutes
- distance: Flight distance in miles
- hour, minute: Scheduled departure time
- And more...
