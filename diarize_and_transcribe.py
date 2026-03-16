from pathlib import Path

from core.media.diarizer import Diarizer
from core.media.transcriber import Transcriber

from core.media.diarizer_backend import PyannoteDiarizerBackend
from core.media.transcriber_backend import FasterWhisperTranscriberBackend


AUDIO_FILE = "audio.wav"


def overlap(a0: float, a1: float, b0: float, b1: float) -> float:
    return max(0.0, min(a1, b1) - max(a0, b0))


def pick_speaker_for_segment(diarization_segments, start: float, end: float) -> str:
    best_speaker = "UNKNOWN"
    best_overlap = 0.0

    for turn in diarization_segments:
        ov = overlap(start, end, turn.start, turn.end)
        if ov > best_overlap:
            best_overlap = ov
            best_speaker = turn.speaker

    return best_speaker


def main():
    audio_path = Path(AUDIO_FILE)

    transcriber = Transcriber(
        backend=FasterWhisperTranscriberBackend(
            model_name="base",
            prefer_gpu=True,
        )
    )

    diarizer = Diarizer(
        backend=PyannoteDiarizerBackend(
            token_env_var="HF_TOKEN",
            prefer_gpu=True,
        )
    )

    diarization = diarizer.diarize(audio_path)
    transcript = transcriber.transcribe(audio_path)

    print("\nSpeakers detected:")
    for seg in diarization.segments:
        print(seg)

    print("\nTranscript:")
    print(transcript.text)

    print("\nSegments:")
    for seg in transcript.segments:
        print(seg)

    print("\nSpeaker Transcript\n")
    for seg in transcript.segments:
        speaker = pick_speaker_for_segment(
            diarization.segments,
            seg.start,
            seg.end,
        )
        print(f"[{seg.start:.2f} - {seg.end:.2f}] {speaker}: {seg.text}")


if __name__ == "__main__":
    main()