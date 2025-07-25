import pandas as pd


def column_values(dataset: pd.DataFrame):
    for col in dataset.columns:
        temp = pd.unique(dataset[col])
        if dataset[col].dtype == object:
            print(f"Column: {col}, has {len(temp)} unique values. {temp}")
        else:
            print(
                f"Column: {col}, has {len(temp)} unique values, {temp.min()}-{temp.max()}"
            )


if __name__ == "__main__":
    dataset = pd.read_csv("./dataset/train.csv")
    print(dataset.shape)
    print(dataset.columns)
    print(dataset.dtypes.value_counts())
    print(column_values(dataset))
    print(f"Datset sadrzi {dataset.isnull().sum().sum()} null vrednosti")
    print(dataset[dataset.duplicated()])
