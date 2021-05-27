from google.cloud import speech
import proto, json
from google.cloud import storage

OUT_BUCKET=__DEFINE_THE_OUTPUT_BUCKET__ # where to write the transcribed - json file and the processed audios

def transcribe(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(event)
    print(f"Processing file: {file['name']}.")

    gs_path = "gs://{}/{}".format(file['bucket'],file['name'])
    client = speech.SpeechClient()


    audio = speech.RecognitionAudio(uri=gs_path)

    
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.MULAW,
        sample_rate_hertz=8000,
        language_code="en-US",
    )

    response = client.recognize(config=config, audio=audio)


    js = json.dumps(json.loads(proto.Message.to_json(response))) # seems an overkill but cleans json from new lines and space

    # write to output bucket
    client = storage.Client()
    bucket_out = client.bucket(OUT_BUCKET)
    bucket_in = client.bucket(file['bucket'])

    blob_trascript = storage.Blob("trasncriptions/{}.json".format(file['name']), bucket_out) # create and write trans to output file in bucket out
    blob_trascript.upload_from_string(js)

    # move processed file to output bucket
    blob_audio = storage.Blob(file['name'], bucket_in)
    bucket_in.copy_blob(blob_audio, bucket_out, new_name="processed-audios/{}".format(blob_audio.name)) # make a copy of pocessed blob to out bucket
    bucket_in.delete_blob(blob_audio.name) # delete audio file from input