import pandas as pd 


def create_dummy(df, varname):
    dummies = pd.get_dummies(df[varname], prefix=varname, drop_first=True)
    df = df.drop(columns=[varname])
    df = pd.concat([df, dummies], axis=1)
    return df, list(dummies.columns)

def sanitize_column_names(df, cols):
    new_cols = []
    for c in cols:
        new_c = c.replace('.', '_').replace(' ', '_')
        if new_c != c:
            df.rename(columns={c: new_c}, inplace=True)
        new_cols.append(new_c)
    return df, new_cols
