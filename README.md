# gbf3-expo-paper-code
This is the repository of the study "Who pays for a protected Earth? Unequal Economic Exposure in the race to GBF Target 3" authored by Adam Poupard and Gabriel Santos Carneiro

## Contents:
- "main_multiscenario.py" contains a code that fetches the "multiscenarios\<Scenario>\main.py" files and compute a given bottom-up calculation for the corresponding "data\Shen_masks\<Scenario>.tif" map. You can edit the list of sceanrios based on given maps (potentially others than Shen et al.'s). Note : We don't reproduce Shen et al. (2023) code, which is accessible online. Running this code will produce intermediary files (maps and country-product output vectors) before agregation and reproduction of the "final_result_XXX.csv" files.

- Data availability : for storage capacity issues, we don't store all the used dataset on this repo. They can be found here, and potentially updated with new versions : [CROPGRIDS](https://openknowledge.fao.org/items/2be22d63-ede3-4f29-b344-f2b0d4bf01ab), [GLW3](https://www.fao.org/land-water/land/land-governance/land-resources-planning-toolbox/category/details/fr/c/1236449/) and [WDPA](https://www.protectedplanet.net/en/thematic-areas/wdpa?tab=WDPA) (for reproductibility : WDPA database, CROPGRIDS version, GLW3 and FAOSTAT were used as of july 2024).

- "multiscenarios" folder contains all the csv files, both sectors-wise aggregated and disaggregated at country-level, for each scenario (Budget = 30%, w in [0, 0.1, 0.2, 0.4, 0.6, 0.8, 1], Scenario in [Global, Country]). These direct exposure vectors are used in the study as an input to the Input-Output analysis after being set in coherence and aggregated with GLORIA agricultural sectors.

- "data\toGLORIA" contains the translation to GLORIA and "GLORIA_computing.R" the code to run the translation and MRIO computation

- "data\toFigures\Results for Figures and Total Results_Nature.xlsx" contains the results underpinning the study submitted to LUP. The first tabs indicate the values used to create each figure. Other tabs are named after the scenarios and displays results at country and sectoral level of disaggregation. There you can find direct exposure estimates, not capped indirect and total exposure estimates, capped indirect and total estimates, sectoral output from GLORIA, and the share of total capped estimates in relation to total sectoral output.

