import pandas as pd
import numpy as np
import os
import re

df_res_full = pd.read_csv(
    r"consommation_data/consommation-annuelle-residentielle-par-adresse.csv",
    sep = ";", 
    dtype = {"Numéro de voie":str}
    )

df_ent_full = pd.read_csv(
    r"consommation_data/consommation-annuelle-entreprise-par-adresse.csv",
    sep = ";", 
    dtype = {"Numéro de voie":str}
    )

year_list = df_res_full["Année"].unique()


# Raw data files & path (.json files in the path)
raw_data_path = "consommation_data"
raw_population_path = "population/POPULATION_MUNICIPALE_COMMUNES_FRANCE.xlsx"

# Processed data files & path
processed_data_path = "ready_to_use_data"

residential_department_path = processed_data_path + "/res_dep_consumption.json"
residential_region_path = processed_data_path + "/res_reg_consumption.json"
sector_departement_path = processed_data_path + "/dep_sectors.json"
sector_region_path = processed_data_path + "/reg_sectors.json"
entreprise_departement_path = processed_data_path + "/ent_dep_consumption.json"
entreprise_region_path = processed_data_path + "/ent_reg_consumption.json"

# log_file = "Transformation.log"  # Fichier de log

# Departement in regions (cf changement 2015 régions française) 
REGIONS = {
    '84': ['01', '03', '07', '15', '26', '38', '42', '43', '63', '69', '73', '74'],
    '27': ['21', '25', '39', '58', '70', '71', '89', '90'],
    '53': ['35', '22', '56', '29'],
    '24': ['18', '28', '36', '37', '41', '45'],
    '94': ['2A', '2B'],
    '44': ['08', '10', '51', '52', '54', '55', '57', '67', '68', '88'],
    '32': ['02', '59', '60', '62', '80'],
    '11': ['75', '77', '78', '91', '92', '93', '94', '95'],
    '28': ['14', '27', '50', '61', '76'],
    '75': ['16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87'],
    '76': ['09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82'],
    '52': ['44', '49', '53', '72', '85'],
    '93': ['04', '05', '06', '13', '83', '84'],
    }



# Modification of the region code for each departement (attention à la version de pandas, pourrait poser problème)

def population_departement_correction(df_pop,reg_list):
    for reg in reg_list:
        dep = REGIONS[reg]
        df_pop.loc[df_pop['dep'].isin(dep), 'reg'] = reg
    return df_pop



# Modification of departements code to comply with string version

def departement_code_transformer(dep_int):
    dep_str = []
    for dep in dep_int:
        if dep < 10:
            dep_str.append("0"+str(dep))
        else:
            dep_str.append(str(dep))
    return dep_str



# Function to process residential data , output the consumption(homes)/person (energy in MWh)

def transfo_residential(df_residential,df_pop,code_zone,zone):
    # if its region data

    if zone=="reg":
        # filter the data to select only the chosen region 

        df_res = df_residential.loc[df_residential['Code Région'] == code_zone]
        
        # calculating the population in the region

        code_zone = str(code_zone)
        df_population = df_pop.loc[df_pop['reg'] == code_zone].groupby("reg", as_index=False).sum()   
        pop_years = [df_population["p18_pop"], df_population["p19_pop"], df_population["p20_pop"], df_population["p21_pop"]]
        pop = np.mean(pop_years)

    # if its departement data

    else:
        # filter the data to select only the chosen departement 

        df_res = df_residential.loc[df_residential['Code Département'] == code_zone]

        # transforming the code to comply with its string version
        if code_zone < 10:
            code_zone ="0"+str(code_zone)
        else:
            code_zone = str(code_zone)

        # calculating the population in the departement

        df_population = df_pop.loc[df_pop['dep'] == code_zone].groupby("dep", as_index=False).sum()   
        pop_years = [df_population["p18_pop"], df_population["p19_pop"], df_population["p20_pop"], df_population["p21_pop"]]
        pop = np.mean(pop_years)
    
    # sum of the consumption (return consumption per person)

    df_res = df_res.groupby("Année", as_index=False).sum()
    Consumption_res = df_res["Consommation annuelle totale de l'adresse (MWh)"].tolist()[0]

    return (Consumption_res/pop)



# Function to process entreprise data , output consumption(entreprise)/person (energy in MWh)

def transfo_entreprise(df_entreprise,df_pop,code_zone,zone):
    # if its region data

    if zone=="reg":
        # filter the data to select only the chosen departement

        df_ent = df_entreprise.loc[df_entreprise['Code Région'] == code_zone]
        
        # calculating the population in the region

        code_zone = str(code_zone)
        df_population = df_pop.loc[df_pop['reg'] == code_zone].groupby("reg", as_index=False).sum()   
        pop_years = [df_population["p18_pop"], df_population["p19_pop"], df_population["p20_pop"], df_population["p21_pop"]]
        pop = np.mean(pop_years)

    # if its departement data

    else:
         # filter the data to select only the chosen departement 
        df_ent = df_entreprise.loc[df_entreprise['Code Département'] == code_zone]

        # transforming the code to comply with its string version
        if code_zone < 10:
            code_zone ="0"+str(code_zone)
        else:
            code_zone = str(code_zone)
        
        # calculating the population in the departement

        df_population = df_pop.loc[df_pop['dep'] == code_zone].groupby("dep", as_index=False).sum()
        pop_years = [df_population["p18_pop"], df_population["p19_pop"], df_population["p20_pop"], df_population["p21_pop"]]
        pop = np.mean(pop_years)
    
    # sum of the consumption (return consumption per person)

    df_ent = df_ent.groupby("Année", as_index=False).sum()
    Consumption_ent = df_ent["Consommation annuelle totale de l'adresse (MWh)"].tolist()[0]

    return (Consumption_ent/pop)



# Function to get raw sectors consumption from the entreprise data (energy in MWh)

def transfo_sectors(df_entreprise,code_zone,zone):
    # filter depending on if its a departement or a region

    if zone=="reg":
        df_sector = df_entreprise.loc[df_entreprise['Code Région'] == code_zone]
    else:
        df_sector = df_entreprise.loc[df_entreprise['Code Département'] == code_zone]
    
    # get the sum of consumption for each sectors

    df_sector = df_sector.groupby("Secteur d'activité", as_index=False).sum()
    sectors = df_sector["Secteur d'activité"].to_list()
    consumption = df_sector["Consommation annuelle totale de l'adresse (MWh)"].to_list()

    # add the missing sectors to the lists

    if "AGRICULTURE" not in sectors:
        sectors.append("AGRICULTURE")
        consumption.append(0)
    if "INDUSTRIE" not in sectors:
        sectors.append("INDUSTRIE")
        consumption.append(0)
    if "INCONNU" not in sectors:
        sectors.append("INCONNU")
        consumption.append(0)
    if "TERTIAIRE" not in sectors:
        sectors.append("TERTIAIRE")
        consumption.append(0)
    
    return sectors,consumption



# Function to process the residential data for a year

def create_data_res(df,df_pop):
    dep_consumption = []
    reg_consumption = []

    region_code = np.unique(df["Code Région"])
    region_code = region_code[~np.isnan(region_code)]
    region_code = region_code.astype(int)

    departement_code = np.unique(df["Code Département"])
    departement_code = departement_code[~np.isnan(departement_code)]
    departement_code = departement_code.astype(int)

    # putting the correct region code for each departement
    df_pop = population_departement_correction(df_pop,region_code.astype(str)) 

    # departement (go through all of them)
    for code_dep in departement_code:
        dep_consumption.append(transfo_residential(df,df_pop,code_dep,"dep"))
    
    # region (go through all of them)
    for code_reg in region_code:
        reg_consumption.append(transfo_residential(df,df_pop,code_reg,"reg"))

    return dep_consumption,reg_consumption,departement_code,region_code



# Function to process the entreprise data for a year (consumption data & sector data)

def create_data_ent(df,df_pop):
    dep_consumption = []
    reg_consumption = []
    dep_df_sectors = pd.DataFrame(columns=["TERTIAIRE","INDUSTRIE","AGRICULTURE","INCONNU"])
    reg_df_sectors = pd.DataFrame(columns=["TERTIAIRE","INDUSTRIE","AGRICULTURE","INCONNU"])
    secor_name = ["TERTIAIRE","INDUSTRIE","AGRICULTURE","INCONNU"]
    
    region_code = np.unique(df["Code Région"])
    region_code = region_code[~np.isnan(region_code)]
    region_code = region_code.astype(int)

    departement_code = np.unique(df["Code Département"])
    departement_code = departement_code[~np.isnan(departement_code)]
    departement_code = departement_code.astype(int)

    # putting the correct region code for each departement
    df_pop = population_departement_correction(df_pop,region_code.astype(str)) 

    # departement (go through all of them)
    for code_dep in departement_code:

        # consumption per person
        dep_consumption.append(transfo_entreprise(df,df_pop,code_dep,"dep"))

        # consumption for each sectors
        row_sector = [0,0,0,0]
        sectors,consumption = transfo_sectors(df,code_dep,"dep")
        for i in range(len(sectors)):
            id = secor_name.index(sectors[i])
            row_sector[id] = consumption[i]
        
        dep_df_sectors.loc[len(dep_df_sectors)] = row_sector

    # region (go through all of them)
    for code_reg in region_code:

        # consumption per person
        reg_consumption.append(transfo_entreprise(df,df_pop,code_reg,"reg"))

        # consumption for each sectors
        row_sector = [0,0,0,0]
        sectors,consumption = transfo_sectors(df,code_reg,"reg")
        for i in range(len(sectors)):
            id = secor_name.index(sectors[i])
            row_sector[id] = consumption[i]
        
        reg_df_sectors.loc[len(reg_df_sectors)] = row_sector

    return dep_consumption,reg_consumption,departement_code,region_code,dep_df_sectors,reg_df_sectors



# Function that accumulate the data for each years and save the processed data

def generate_json(yrs):

    # creating the dataframes that will hold the data to save as json

    df_res_dep = pd.DataFrame(columns=["department","year","consumption_per_resident"])
    df_res_reg = pd.DataFrame(columns=["region","year","consumption_per_resident"])

    df_ent_dep = pd.DataFrame(columns=["department","year","consumption_per_resident"])
    df_ent_reg = pd.DataFrame(columns=["region","year","consumption_per_resident"])

    df_sectors_dep = pd.DataFrame(columns=["TERTIAIRE","INDUSTRIE","AGRICULTURE","INCONNU","departement","year"])
    df_sectors_reg = pd.DataFrame(columns=["TERTIAIRE","INDUSTRIE","AGRICULTURE","INCONNU","region","year"])

    # getting the population data

    df_pop = pd.read_excel(raw_population_path)

    for yr in yrs:
        
        year = yr

        # regex to verify the currently analyzed file is the correct one (will ignore non conforming file name)

        regex_res = re.compile("consommation-annuelle-residentielle")
        regex_ent = re.compile("consommation-annuelle-entreprise")



        df_res = df_res_full[df_res_full["Année"] == year]

        # if file is empty then ignore

        if len(df_res) >0:

            # process to get the residential processed data
            dep_consumption,reg_consumption,departements,regions = create_data_res(df_res,df_pop)

            # get the departement and region code in the correct type to save in the dataframe (then json)
            departements = departement_code_transformer(departements)
            regions = regions.astype(str)
                
            # append the processed data to the dataframes (who will be saved as json at the end)

            res_dep = pd.DataFrame({"department":departements,"year":[year]*len(departements),"consumption_per_resident":dep_consumption})
            res_reg = pd.DataFrame({"region":regions,"year":[year]*len(regions),"consumption_per_resident":reg_consumption})

            df_res_dep = pd.concat([df_res_dep,res_dep],ignore_index=True)
            df_res_reg = pd.concat([df_res_reg,res_reg],ignore_index=True)
            

        df_ent = df_ent_full[df_ent_full["Année"] == year]

        # if file is empty then ignore

        if len(df_ent) >0:

            # process to get the entreprise processed data (consumption plus sectors)
            dep_consumption,reg_consumption,departements,regions,dep_df_sectors,reg_df_sectors = create_data_ent(df_ent,df_pop)

            # get the departement and region code in the correct type to save in the dataframe (then json)
            departements = departement_code_transformer(departements)
            regions = regions.astype(str)

            # append the processed data to the dataframes (who will be saved as json at the end)

            ent_dep = pd.DataFrame({"department":departements,"year":[year]*len(departements),"consumption_per_resident":dep_consumption})
            ent_reg = pd.DataFrame({"region":regions,"year":[year]*len(regions),"consumption_per_resident":reg_consumption})
            dep_df_sectors["departement"] = departements
            reg_df_sectors["region"] = regions
            dep_df_sectors["year"] = [year]*len(departements)
            reg_df_sectors["year"] = [year]*len(regions)

            # consumption
            df_ent_dep = pd.concat([df_ent_dep,ent_dep],ignore_index=True)
            df_ent_reg = pd.concat([df_ent_reg,ent_reg],ignore_index=True)

            # sector
            df_sectors_dep = pd.concat([df_sectors_dep,dep_df_sectors],ignore_index=True)
            df_sectors_reg = pd.concat([df_sectors_reg,reg_df_sectors],ignore_index=True)


    df_res_dep = df_res_dep.to_csv(processed_data_path+"/res_dep_consumption.csv", index=False)
    df_res_reg = df_res_reg.to_csv(processed_data_path+"/res_reg_consumption.csv", index=False)
    
    df_sectors_dep = df_sectors_dep.to_csv(processed_data_path+"/dep_sectors.csv", index=False)
    df_sectors_reg = df_sectors_reg.to_csv(processed_data_path+"/reg_sectors.csv", index=False)

    df_ent_dep = df_ent_dep.to_csv(processed_data_path+"/ent_dep_consumption.csv", index=False)
    df_ent_reg = df_ent_reg.to_csv(processed_data_path+"/ent_reg_consumption.csv", index=False)
    



# Check for requirement and validation

def save_data():
    # check if folder to save data exist (create it if not)
    if not os.path.exists(processed_data_path):
        os.makedirs(processed_data_path)
    print("Starting transformation")
    generate_json(year_list)
    print("Transformation done")
    return

save_data()