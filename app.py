from flask import Flask, render_template, request
import boto3

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True


@app.route('/')
def home_page():
    return render_template("home.html")


@app.route('/upload', methods=["POST"])
def uploader():
    s3 = boto3.client('s3', aws_access_key_id="AKIAVBROUFH5XCQ5GJ47", aws_secret_access_key="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")
    f = request.files['filename']
    if f.content_type == 'text/csv':
    #if f.content_type in ['image/png', 'image/jpeg', 'image/jpg']:
        s3.upload_fileobj(f, 'aws-bucky7', f.filename)
        return render_template("uploaded.html")

    # if file is not chosen or other file format in entered then take them to error page to upload again option
    else:
        return render_template("Tryagain.html")

@app.route('/gallery')
def gallery():
    # Retrieve a list of image keys from your S3 bucket
    s3 = boto3.client('s3', aws_access_key_id="AKIAVBROUFH5XCQ5GJ47", aws_secret_access_key="2sv3ZFh85LenxziZblkAqMrXGidh2SwuZVrxHOuk")
    bucket_name = 'aws-bucky7'
    # to list all the object in the bucket
    objects = s3.list_objects(Bucket=bucket_name)
    contents = []  # to store all the keys
    for i in objects['Contents']:  # to retrieve all the keys in S3 bucket
        contents.append(i['Key'])

    # Render the gallery template with the list of image keys
    return render_template('gallery.html', contents=contents)
# for the gallery page to show all the images of the product

if __name__ == "__main__":
    app.run(debug=True)
