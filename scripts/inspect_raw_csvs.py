import pandas as pd

FILES = [
    "data/raw/anime.csv",
    "data/raw/anime_genres.csv",
    "data/raw/anime_companies.csv",
    "data/raw/anime_voice_actors.csv",
    "data/raw/entities.csv",
]

def inspect(path: str, n: int = 3) -> None:
    df = pd.read_csv(path)
    print("=" * 80)
    print(path)
    print("- columns:", list(df.columns))
    print("- shape:", df.shape)
    print("- dtypes:\n", df.dtypes)
    print("- head:\n", df.head(n))

def main():
    for f in FILES:
        inspect(f)

if __name__ == "__main__":
    main()