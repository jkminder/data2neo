import pandas as pd

## test input data ##
schema_file_name = "tests/integration/resources/schema_end_to_end.yaml"

no_duplicates = ("Person", pd.DataFrame({
    "ID": [1,2,3,4],
    "FirstName": ["Julian", "Fritz",  "Hans", "Rudolfo"],
    "LastName": ["Minder", "SomeGuy", "Müller",  "Muster"],
    "FavoriteFlower": ["virginica", "setosa", "versicolor", "setosa"]
}))

duplicates = ("Person", pd.DataFrame({
        "ID": [1,2,2,3,4,4,4,4],
        "FirstName": ["Julian", "Fritz",  "Fritz", "Hans", "Rudolfo", "Rudolfo", "Rudolfo", "Rudolfo"],
        "LastName": ["Minder", "SomeGuy", "SomeGuy", "Müller", "Muster", "Muster", "Muster", "Muster"],
        "FavoriteFlower": ["virginica", "setosa", "setosa", "versicolor", "setosa", "setosa", "setosa", "setosa"]
}))

before_update = ("Person", pd.DataFrame({
        "ID": [1,2,3,4],
        "FirstName": ["Julian", "Fritz",  "Hans", "Rudolfo"],
        "LastName": ["AnotherName", "Generic",  "Müller", "MusterMann"],
        "FavoriteFlower": ["no favorite", "setosa", "versicolor", "setosa"]
})) # data used to setup initial data which is later updated

iris = ("Flower", pd.DataFrame({
    "sepal_length" : [0.1, 0.2, 0.3, 0.4, 0.5],
    "sepal_width": [1.1, 1.2, 1.3, 1.4, 1.5],
    "petal_length": [2.1, 2.2, 2.3, 2.4, 2.5],
    "petal_width": [3.1, 3.2, 3.3, 3.4, 3.5],
    "random_property": [1,2,3,4,5],
    "species": ["setosa", "setosa", "versicolor", "virginica", "roseabluma"],
}))

## test result data ##
person_nodes = [
        (["Person"], {"ID": 1, "FirstName": "Julian", "Renamed": "Minder", "Static": "staticstring"}),
        (["Person"], {"ID": 2, "FirstName": "Fritz", "Renamed": "SomeGuy", "Static": "staticstring"}),
        (["Person"], {"ID": 3, "FirstName": "Hans", "Renamed": "Müller", "Static": "staticstring"}),
        (["Person"], {"ID": 4, "FirstName": "Rudolfo", "Renamed": "Muster", "Static": "staticstring"})
]

species_nodes = [
        (["Species", "BioEntity", "versicolor"], {"Name": "versicolor"}),
        (["Species", "BioEntity", "virginica"], {"Name": "virginica"}),
        (["Species", "BioEntity", "roseabluma"], {"Name": "roseabluma"}),
        (["Species", "BioEntity", "roseabluma"], {"Name": "roseabluma"}),
]

flower_nodes = [(["Flower"], {"sepal_length": f["sepal_length"], "sepal_width": f["sepal_width"], 
        "petal_length": f["petal_length"], "petal_width": f["petal_width"]}) for f in iris[1].iloc]

is_relations = [((["Flower"], {"sepal_length": f["sepal_length"], "sepal_width": f["sepal_width"], 
        "petal_length": f["petal_length"], "petal_width": f["petal_width"]}),"is", 
        (["Species", "BioEntity", f["species"]], {"Name": f["species"]}), {}) for f in iris[1].iloc]

likes_relations = [((["Person"], {"ID": p["ID"], "FirstName": p["FirstName"], 
        "Renamed": p["LastName"], "Static": "staticstring"}),"likes", 
        (["Species", "BioEntity", p["FavoriteFlower"]], {"Name": p["FavoriteFlower"]}), {"Since":"4ever", "EntityAttribute": p["ID"]}) for p in no_duplicates[1].iloc]

likes_relations_parallel = [((["Person"],  {"ID": 1, "FirstName": "Julian", "Renamed": "Minder", "Static": "staticstring"}),"likes_parallel", 
        (["Species", "BioEntity", "virginica"], {"Name": "virginica"}), {"pk": i}) for i in [1,2,3,4]]

likes_relations_parallel = [((["Person"],  {"ID": 1, "FirstName": "Julian", "Renamed": "Minder", "Static": "staticstring"}),"likes_parallel", 
        (["Species", "BioEntity", "virginica"], {"Name": "virginica"}), {"pk": i}) for i in [1,2,3,4]]

likes_relations_merged = [((["Person"],  {"ID": 1, "FirstName": "Julian", "Renamed": "Minder", "Static": "staticstring"}),"likes_merged", 
        (["Species", "BioEntity", "virginica"], {"Name": "virginica"}), {"pk": 1})]

person_only_nodes_only_result = {
    "nodes": person_nodes, 
    "relations": []
}

flower_only_result = {
    "nodes": flower_nodes + species_nodes,
    "relations": is_relations
}

full_result = {
    "nodes": person_nodes + species_nodes + flower_nodes, 
    "relations": is_relations + likes_relations
}

result_parallel = {
    "nodes": person_nodes + species_nodes, 
    "relations": likes_relations_parallel + likes_relations_merged
}


######################