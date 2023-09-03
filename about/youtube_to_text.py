import whisper
import ffmpeg

# mp3_file = ffmpeg.input(r"C:\Users\logan\Downloads\onlymp3.to - What Alcohol Does to Your Body, Brain & Health  Huberman Lab Podcast #86-DkS1pkKpILY-256k-1654608736116.mp3")
# mp3_file = 
model = whisper.load_model("base")
result = model.transcribe("huberman_test.mp3")
print(result["text"])