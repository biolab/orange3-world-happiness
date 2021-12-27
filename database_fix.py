from orangecontrib.owwhstudy.whstudy import WorldIndicators
import wbgapi as wb
if __name__ == "__main__":
    handle = WorldIndicators("main", "biolab")
    indicators = [code for (code, _, _, _) in handle.indicators()]
    countries = [code for (code, _) in handle.countries()]
    years = list(range(1960, 2020))
    handle.update(countries, indicators, years, 'WDI')





