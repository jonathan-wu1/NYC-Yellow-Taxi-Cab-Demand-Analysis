[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/LOuMvgtV)
[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=11487060&assignment_repo_type=AssignmentRepo)
# MAST30034 Project 1 README.md
- Name: Jonathan Wu
- Student ID: ... 

To run the pipeline, please visit the `scripts` directory and run the files in order:
1. `download.py`: This downloads the raw data into the `data/landing` directory.
2. `raw_preprocessing.py`: This removes unwanted columns and moves the resulting data into the `data/raw` directory.
3. `curating.py`: This removes outlier rows and moves the resulting data into the `data/curated` directory.

Please visit the `notebooks` directory and run the files in order:
1. `curating_nb_sql.ipynb`: Further aggregates data for hourly demand and creates multiple new files in the `data/curated` directory.
2. `analysis_nb_demand.ipynb`: Analysis on the hourly demand vs weekday and zones 
3. `modelling_nb_1.ipynb`: Creates a linear regression model to predict hourly demand. Regression model is used to test the predictors significance.
4. `modelling_nb_1.ipynb`: Creates a decision tree regression model to predict hourly demand. 





