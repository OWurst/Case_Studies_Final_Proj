import dataloader
from dataloader import DataLoader, log
import tqdm
import numpy as np
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    precision_recall_fscore_support,
    confusion_matrix,
    classification_report
)
from tensorflow import keras
import joblib

"""
this class takes in a model, trains it, saves it to a file, evaluates it, etc
"""

TRAIN_DATA_PCT = 0.7
TEST_DATA_PCT = 0.15
VALIDATION_DATA_PCT = 0.15

class keras_wrapper():
    def __init__(self, model):
        if model is None:
            # try to load existing model
            try:
                self.model = keras.models.load_model('keras_wrapper.h5')
                log("Loaded existing Keras model from keras_wrapper.h5")
            except Exception as e:
                log(f"Failed to load existing model: {e}")
                raise ValueError("No model provided and failed to load existing model.")
        else:
            self.model = model

    def is_fitted(self):
        """Check if the Keras model has been trained (has weights)."""
        try:
            return len(self.model.weights) > 0
        except:
            return False

    def ensure_fitted(self):
        """Try to load model if not fitted."""
        if not self.is_fitted():
            try:
                self.model = keras.models.load_model('keras_wrapper.h5')
                log("Loaded existing Keras model from keras_wrapper.h5")
            except Exception as e:
                raise ValueError(f"Model is not fitted and cannot load pre-trained model: {e}")

    def train(self, X, y):
        self.model.train_on_batch(X, y)

    def predict(self, X, y):
        self.ensure_fitted()
        return np.argmax(self.model.predict(X, verbose=0), axis=-1)

    def save(self):
        self.model.save(self.model.__class__.__name__ + '.h5')

    def load(self, filename):
        self.model = keras.models.load_model(filename)

class sklearn_wrapper():
    def __init__(self, model, classes=[0, 1, 2]):
        if model is None:
            # try to load existing model
            try:
                self.model = joblib.load('sklearn_wrapper.joblib')
                log("Loaded existing sklearn model from sklearn_wrapper.joblib")
            except Exception as e:
                log(f"Failed to load existing model: {e}")
                raise ValueError("No model provided and failed to load existing model.")
        else:
            self.model = model
            self.first_fit = True
            self.classes = classes

    def is_fitted(self):
        """Check if the sklearn model has been fitted."""
        try:
            from sklearn.utils.validation import check_is_fitted
            check_is_fitted(self.model)
            return True
        except:
            return False

    def ensure_fitted(self):
        """Try to load model if not fitted."""
        if not self.is_fitted():
            try:
                self.model = joblib.load('sklearn_wrapper.joblib')
                log("Loaded existing sklearn model from sklearn_wrapper.joblib")
            except Exception as e:
                raise ValueError(f"Model is not fitted and cannot load pre-trained model: {e}")

    def train(self, X, y):
        if self.first_fit:
            self.model.fit(X, y)
            self.first_fit = False
        else:
            self.model.partial_fit(X, y, classes=self.classes)

    def predict(self, X, y):
        self.ensure_fitted()
        return self.model.predict(X)
    
    def save(self):
        joblib.dump(self.model, self.model.__class__.__name__ + '.joblib')

    def load(self, filename):
        import joblib
        self.model = joblib.load(filename)

class Modeler():
    def __init__(self, model=None, trainer_type='keras'):
        if trainer_type == 'keras':
            self.model = keras_wrapper(model)
        elif trainer_type == 'sklearn':
            self.model = sklearn_wrapper(model)
        else:
            raise ValueError("Invalid trainer type. Must be 'keras' or 'sklearn'.")
        self.dataloader = DataLoader()

    def training_loop(self, num_epochs=100, data_type='simple'):
        log(f"Starting training loop for {num_epochs} epochs on {data_type} data for model {self.model.__class__.__name__}  ...")
        
        for epoch in tqdm.tqdm(range(num_epochs), desc="Training"):
            if data_type == 'simple':
                batches, total_records = self.dataloader.setup_simple()
            elif data_type == 'graph':
                batches, total_records = self.dataloader.setup_graph()
            else: 
                raise ValueError("Invalid data type. Must be 'simple' or 'graph'.")
            
            log(f"Epoch {epoch+1}/{num_epochs} - Total Records: {total_records}, Batches: {batches}")
            
            for batch_num in tqdm.tqdm(range(batches), desc="Batches"):
                if data_type == 'simple':
                    X, y = self.dataloader.get_next_page_simple(process='train')
                elif data_type == 'graph':
                    X, y = self.dataloader.get_next_page_graph(process='train')
                
                if X is None or y is None:
                    log("No more data to load. Ending epoch early.")
                    break
                
                self.model.train(X, y)
        
        log("Training complete for model " + self.model.__class__.__name__)
        # save the model after training
        self.model.save()

    def tune_hyperparameters(self):
        pass

    def evaluate(self, data_type='simple', model_name=None):
        log(f"Starting evaluation on {data_type} data for model {self.model.__class__.__name__}  ...")
        
        # Check if model is fitted, try to load if not
        if isinstance(self.model, sklearn_wrapper):
            try:
                from sklearn.utils.validation import check_is_fitted
                check_is_fitted(self.model.model)
            except Exception as e:
                log(f"Model not fitted ({e}), attempting to load from disk...")
                try:
                    model_filename = self.model.model.__class__.__name__ + '.joblib'
                    self.model.model = joblib.load(model_filename)
                    log(f"Successfully loaded sklearn model from {model_filename}")
                except Exception as load_e:
                    raise ValueError(f"Model is not fitted and failed to load: {load_e}")
        elif isinstance(self.model, keras_wrapper):
            try:
                if not (hasattr(self.model.model, 'weights') and len(self.model.model.weights) > 0):
                    raise ValueError("Model has no weights")
            except Exception as e:
                log(f"Model not fitted, attempting to load from disk...")
                try:
                    model_filename = self.model.model.__class__.__name__ + '.h5'
                    self.model.model = keras.models.load_model(model_filename)
                    log(f"Successfully loaded Keras model from {model_filename}")
                except Exception as load_e:
                    raise ValueError(f"Model is not fitted and failed to load: {load_e}")
        
        if data_type == 'simple':
            batches, total_records = self.dataloader.setup_simple()
        elif data_type == 'graph':
            batches, total_records = self.dataloader.setup_graph()
        else: 
            raise ValueError("Invalid data type. Must be 'simple' or 'graph'.")
        
        log(f"Total Records: {total_records}, Batches: {batches}")
        
        y_true_all = []
        y_pred_all = []

        for batch_num in tqdm.tqdm(range(batches), desc="Eval Batches"):
            if data_type == 'simple':
                X, y = self.dataloader.get_next_page_simple(process='test')
            elif data_type == 'graph':
                X, y = self.dataloader.get_next_page_graph(process='test')
            
            if X is None or y is None:
                # if this isnt the last batch we just continue
                if batch_num != batches - 1:
                    continue

                log("No more data to load. Ending evaluation early.")
                break

            if model_name == 'NB':
                # make all values non-negative for NB
                X = np.where(X < 0, 0, X)

            y_pred = self.model.predict(X, y)
            y_true_all.extend(np.asarray(y).tolist())
            y_pred_all.extend(y_pred.tolist())

        # Calculate evaluation metrics
        accuracy = accuracy_score(y_true_all, y_pred_all)
        balanced_accuracy = balanced_accuracy_score(y_true_all, y_pred_all)
        precision, recall, f1, support = precision_recall_fscore_support(y_true_all, y_pred_all)
        cm = confusion_matrix(y_true_all, y_pred_all)
        report = classification_report(y_true_all, y_pred_all)

        results = {
            'accuracy': accuracy,
            'balanced_accuracy': balanced_accuracy,
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
            'support': support,
            'confusion_matrix': cm,
            'classification_report': report
        }

        return results


