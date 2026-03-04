import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:root@localhost:5432/fraud_db")

df = pd.read_sql("SELECT * FROM fraud_features", engine)

print(df.shape)
print(df.head())
df = df.fillna(0)

features = [
    'transactionamt',
    'avg_amt_per_card',
    'amt_deviation',
    'device_risk_score'
]

X = df[features]
y = df['isfraud']

print(X.shape)
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42
)
from sklearn.linear_model import LogisticRegression

model = LogisticRegression(max_iter=1000)

model.fit(X_train, y_train)
y_pred = model.predict(X_test)
from sklearn.metrics import accuracy_score

accuracy = accuracy_score(y_test, y_pred)

print("Model Accuracy:", accuracy)
df['fraud_probability'] = model.predict_proba(X)[:,1]
def risk_band(prob):
    if prob < 0.30:
        return "LOW"
    elif prob < 0.70:
        return "MEDIUM"
    else:
        return "HIGH"

df['risk_band'] = df['fraud_probability'].apply(risk_band)
risk_df = df[[
    'transactionid',
    'transactionamt',
    'device_risk_score',
    'fraud_probability',
    'risk_band',
    'isfraud'
]]
risk_df.to_sql(
    "fraud_risk_scores",
    engine,
    if_exists="replace",
    index=False
)
