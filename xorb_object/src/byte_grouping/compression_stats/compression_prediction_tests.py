import argparse
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix

# TO USE: 
#
# python compression_prediction_tests.py compression_stats.csv

def main():
    parser = argparse.ArgumentParser(description="Train a logistic regression model to predict when BG4 is >10% better.")
    parser.add_argument("csv_file", type=str, help="Path to the CSV file with block analysis results.")
    args = parser.parse_args()
    
    # 1. Load data
    df = pd.read_csv(args.csv_file)
    
    # 2. Create the target: is size_scheme_2 at least 10% smaller than size_scheme_1?
    #    Equivalently, is size_scheme_2 < 0.9 * size_scheme_1?
    #    We'll define a 'big_improvement' column: 1 if BG4 is >10% better, else 0.
    df["improvement"] = (
        df["size_scheme_2"] < df["size_scheme_1"]
    ).astype(int)
    
    df["big_improvement"] = (
        df["size_scheme_2"] < 0.95 * df["size_scheme_1"]
    ).astype(int)
    
    df["possible_improvement"] = (
        df["size_scheme_2"] < 1.05 * df["size_scheme_1"]
    ).astype(int)
    
    slice_cols = ["slice_0_entropy", "slice_1_entropy", "slice_2_entropy", "slice_3_entropy"]
    
    df["min_slice_entropy"] = df[slice_cols].min(axis=1) - df["full_entropy"]
    df["max_slice_entropy"] = df[slice_cols].max(axis=1) - df["full_entropy"]
    
    slice_cols = ["slice_0_kl", "slice_1_kl", "slice_2_kl", "slice_3_kl"]
    
    df["max_kl"] = df[slice_cols].max(axis=1)

    # 3. Select features to use.  This one is simply to use the maximum kl divergence as the 
    #    sole feature.
    features = [
            "max_kl",
            ] 

    X = df[features]
    y = df["improvement"]
    
    
    # 5. Train Logistic Regression
    clf = LogisticRegression(max_iter=1000, random_state=42)
    clf.fit(X, y)
 
    # 6. Evaluate.  Here, because it is a super simple model, just use the whole data to evaluate it instead 
    #    of bothering with a train/test split. 
    y_pred = clf.predict(X)

    # This rule is what the learned regression above uses.
    # y_pred = (X["max_kl"] > 0.02).astype(int) 

    accuracy = accuracy_score(y, y_pred)
    print("Accuracy on test set:", accuracy)
    print(classification_report(y, y_pred))
    
    print("\nAccuracy on big improvement", accuracy)
    print(classification_report(df["big_improvement"], y_pred))
    
    cm = confusion_matrix(df["big_improvement"], y_pred)
    tn, fp, fn, tp = cm.ravel()
    
    print(f"Incorrectly predicted {fn} / {fn + tn} of cases where bg4_lz4_size < 0.95 * lz4_size")

    
    print("\nAccuracy on possible improvement", accuracy)
    print(classification_report(df["possible_improvement"], y_pred))
    
    cm = confusion_matrix(df["possible_improvement"], y_pred)
    tn, fp, fn, tp = cm.ravel()
    
    print(f"Incorrectly predicted {fp} / {fp + tp} of cases where bg4_lz4_size > 1.05 * lz4_size")

    print("Coefficients:", clf.coef_)
    print("intercept:", clf.intercept_)


if __name__ == "__main__":
    main()

