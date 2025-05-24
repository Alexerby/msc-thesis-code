from descriptives.helpers import load_data
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay, classification_report

# Load and prepare data
df = load_data("bafoeg_calculations", from_parquet=True)

# Focus on relevant columns
cols = ["theoretical_eligibility", "received_bafög"]
df = df[cols].dropna()

# Extract ground truth and model predictions
y_true = df["received_bafög"].astype(int)               # Actual BAföG receipt (0 = No, 1 = Yes)
y_pred = df["theoretical_eligibility"].astype(int)      # Model-based eligibility (0 = No, 1 = Yes)

# Compute confusion matrix
cm = confusion_matrix(y_true, y_pred)
tn, fp, fn, tp = cm.ravel()

print("\n=== Confusion Matrix ===")
print(f"""
                 Predicted
               |  Not Eligible  |  Eligible
  ------------------------------------------
  Not Received |   TN = {tn:>4}       |  FP = {fp:>4}
    Received   |   FN = {fn:>4}       |  TP = {tp:>4}
""")

# Print formulas and computed metrics
accuracy = (tp + tn) / (tp + tn + fp + fn)
precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0
f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

print("=== Key Metrics ===")
print(f"Accuracy  = (TP + TN) / Total         = ({tp} + {tn}) / {tp + tn + fp + fn} = {accuracy:.3f}")
print(f"Precision = TP / (TP + FP)            = {tp} / ({tp} + {fp}) = {precision:.3f}")
print(f"Recall    = TP / (TP + FN)            = {tp} / ({tp} + {fn}) = {recall:.3f}")
print(f"F1 Score  = 2 * (Prec × Recall) / (Prec + Recall) = {f1:.3f}")

# Print sklearn classification report for both classes
print("\n=== Classification Report ===")
print(classification_report(y_true, y_pred, target_names=["Did not receive", "Received"]))

# Optional visualization
ConfusionMatrixDisplay.from_predictions(y_true, y_pred)
