import optuna
from xgboost import XGBRegressor
import pandas as pd
from typing import Optional
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error 
import numpy as np
from loguru import logger

class XGBoostModel:
    def __init__(self):
        #self.model = XGBRegressor() # We want to perform hyperparameter tuning so we don't initialize the model here
        self.model: XGBRegressor = None # We could lead with an underscore to indicate that this is a private variable not to be accessed directly 

    def fit(self,
            X_train: pd.DataFrame,
            y_train: pd.Series,
            n_search_trials: Optional[int] = 0, # Number of search trials for hyperparameter tuning
            n_splits: Optional[int] = 3 # Number of splits for cross-validation
            ):
        """
        Fit the XGBoost model to the training data
        Args:
            X_train (pd.DataFrame): The input features
            y_train (pd.Series): The target variable
            n_search_trials (int): The number of search trials for hyperparameter tuning
            n_splits (int): The number of splits for cross-validation
        """
        logger.info(f"Training the model with {n_search_trials} search trials and {n_splits} splits")

        # Check the number of search trials and splits are a positive integer
        assert n_search_trials >= 0, "The number of search trials must be a positive integer"
        assert n_splits > 0, "The number of splits for cross-validation must be a positive integer"

        if n_search_trials == 0:
            # Train a model without hyperparameter tuning; default hyperparameters are used
            self.model = XGBRegressor()
            self.model.fit(X_train, y_train)
        else:
            # Train a model with hyperparameter tuning using cross-validation
            # We search for the best hyperparmeters using bayesian optimization
            best_hyperparams = self._find_best_hyperparameters(X_train, y_train, n_search_trials, n_splits)
            logger.info(f"Best hyperparameters found: {best_hyperparams}")

            # Train the model with the best hyperparameters and the full training set
            self.model = XGBRegressor(**best_hyperparams)
            self.model.fit(X_train, y_train)
            logger.info("Model trained with best hyperparameters")
    
    def _find_best_hyperparameters(self, X_train, y_train, n_search_trials, n_splits):
        """
        Find the best hyperparameters for the XGBoost model using Bayesian optimizatio

        Args:
            X_train (_type_): _description_
            y_train (_type_): _description_
            n_search_trials (_type_): _description_
            n_splits (_type_): _description_

        Returns:
            dict: The best hyperparameters found by the optimization
        """
        # Define the objective function to minimize
        def objective(trial: optuna.Trial) -> float:
            """
            Objective function for optuna to that returns the mean absolute error of the model we want to minimize
            
            Args:
                trial (optuna.Trial): An optuna trial object that stores the hyperparameters to search
            
            Returns:
                float: The mean absolute error of the model
            """
            # Define the hyperparameters to search
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
                'max_depth': trial.suggest_int('max_depth', 3, 10),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                'subsample': trial.suggest_float('subsample', 0.5, 1),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
            }

            # Split our X_train into the number of splits
            # We need to keep the time order of the data
            # We use the TimeSeriesSplit to split the data
            tscv = TimeSeriesSplit(n_splits=n_splits)
            mae_scores = []
            for train_index, val_index in tscv.split(X_train):
                # Split the data into training and test sets for this fold
                X_train_fold, X_val_fold = X_train.iloc[train_index], X_train.iloc[val_index]
                y_train_fold, y_val_fold = y_train.iloc[train_index], y_train.iloc[val_index]

                # Train the model on the training set
                model = XGBRegressor(**params) # Pass the hyperparameters to the model and unpack the dictionary using **
                model.fit(X_train_fold, y_train_fold)

                # Evaluate the model on the test set
                y_pred = model.predict(X_val_fold)
                mae = mean_absolute_error(y_val_fold, y_pred)
                mae_scores.append(mae)
            
            # Return the mean of the mae scores across all folds
            return np.mean(mae_scores)

        # Create a study object and optimize the objective function
        study = optuna.create_study(direction='minimize') # We want to minimize the mean absolute error
    
        # We run the optimization for n_search_trials
        study.optimize(objective, n_trials=n_search_trials)
    
        # Get the best hyperparameters
        return study.best_trial.params

    def predict(self, X_test):
        return self.model.predict(X_test)