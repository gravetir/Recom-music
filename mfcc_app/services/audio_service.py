import librosa
import numpy as np
import os

def analyze_audio(file_path):
    """Анализ аудиофайла и извлечение мел-кепстральных характеристик"""
    try:
        y, sr = librosa.load(file_path, sr=44100)
        mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=50)
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        spectral = librosa.feature.spectral_centroid(y=y, sr=sr)
        mel = librosa.feature.melspectrogram(y=y, sr=sr)
        tempo = librosa.beat.tempo(y=y, sr=sr)[0]

        return {
            'mfcc': np.mean(mfccs, axis=1).tolist(),
            'chroma': np.mean(chroma, axis=1).tolist(),
            'spectral_centroid': float(np.mean(spectral)),
            'melspectrogram': float(np.mean(mel)),
            'bpm': round(float(tempo))
        }
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None
