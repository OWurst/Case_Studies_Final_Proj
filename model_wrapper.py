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

    def train(self, X, y):
        self.model.train_on_batch(X, y)

    def predict(self, X, y):
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

    def train(self, X, y):
        if self.first_fit:
            self.model.fit(X, y)
            self.first_fit = False
        else:
            self.model.partial_fit(X, y, classes=self.classes)

    def predict(self, X, y):
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
            
            self.dataloader.reset_offsets()  # reset offsets after each epoch to start from the beginning of the data
        
        log("Training complete for model " + self.model.__class__.__name__)
        # save the model after training
        self.model.save()

    def tune_hyperparameters(self):
        pass

    def evaluate(self, data_type='simple'):
        log(f"Starting evaluation on {data_type} data for model {self.model.__class__.__name__}  ...")
        
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
                log("No more data to load. Ending evaluation early.")
                break

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


