from faker import Faker
import random
import pandas as pd
from datetime import datetime, timedelta
import os
from pathlib import Path


class FakeDataGenerator:
    def __init__(self, data_dir: str = "/opt/spark/work-dir/data/carolinarellano/spotify_logs"):
        self.data_dir = Path(data_dir)
        self.fake = Faker()
        self.genres = ['pop', 'rock', 'k-pop', 'hiphop', 'electronic', 'classical', 'latin', 'jazz', 'country']
        self.event_types = ['play', 'skip', 'like', 'pause', 'seek']
        self.devices = ['mobile', 'desktop', 'tv', 'web player']
        self._setup_directory()
    
    def _setup_directory(self):
        """Create the data directory if it doesn't exist."""
        if not self.data_dir.exists():
            print(f"Creating directory: {self.data_dir}")
            self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_tracks(self, n_tracks: int = 500):
        """Generate fake music track data."""
        tracks = []
        for i in range(n_tracks):
            track_id = f"track_{i:04d}"
            tracks.append({
                'track_id': track_id,
                'artist': self.fake.name(),
                'album': self.fake.sentence(nb_words=3).replace('.', ''),
                'duration_ms': random.randint(90000, 300000),
                'genre': random.choice(self.genres)
            })
        return pd.DataFrame(tracks)
    
    def generate_user_events(self, n_users: int = 200, n_events: int = 20000, 
                           n_tracks: int = 500, start_date: datetime = None):
        """Generate fake user event data."""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=90)
        
        events = []
        for i in range(n_events):
            user_id = f"user_{random.randint(0, n_users-1):04d}"
            track_id = f"track_{random.randint(0, n_tracks-1):04d}"
            ts = start_date + timedelta(seconds=random.randint(0, 90*24*3600))
            events.append({
                'user_id': user_id,
                'track_id': track_id,
                'event_type': random.choices(self.event_types, weights=[0.6, 0.2, 0.1, 0.05, 0.05])[0],
                'event_ts': ts,
                'session_id': f"session_{random.randint(0, 5000):06d}",
                'device': random.choice(self.devices)
            })
        return pd.DataFrame(events)
    
    def save_data_to_csv(self, dataframe: pd.DataFrame, filename: str):
        """Save DataFrame to CSV file in the data directory."""
        file_path = self.data_dir / filename
        dataframe.to_csv(file_path, index=False)
        print(f"Saved {filename} with {len(dataframe)} rows to {file_path}")
        return file_path
    
    def generate_and_save_all(self, n_tracks: int = 500, n_users: int = 200, 
                             n_events: int = 20000, start_date: datetime = None):
        """Generate all fake data and save to CSV files."""
        print("Generating fake data...")
        
        # Generate tracks
        tracks_df = self.generate_tracks(n_tracks)
        tracks_file = self.save_data_to_csv(tracks_df, 'tracks.csv')
        
        # Generate events
        events_df = self.generate_user_events(n_users, n_events, n_tracks, start_date)
        events_file = self.save_data_to_csv(events_df, 'user_events.csv')
        
        print(f"Data generation complete!")
        return tracks_df, events_df


def main():
    """Main function to run the fake data generator."""
    generator = FakeDataGenerator()
    tracks_df, events_df = generator.generate_and_save_all(
        n_tracks=500,
        n_users=200, 
        n_events=20000
    )


if __name__ == "__main__":
    main()