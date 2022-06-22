
# INITIALISATION DE SPARK
from pyspark import SparkContext
from pyspark.sql import SparkSession


def start_spark() -> SparkSession:
    """
    Returns: (SparkSession) my Spark session
    """
    sc = SparkContext()
    sc.setLogLevel("ERROR")
    session = SparkSession(sparkContext=sc)
    print(f"\n{'#' * 100} \n SPARKY'FRUIT 360\n{'#' * 100}\n")
    return session


# ENCODER
"""
Pre-trained deep neural network for image encoding.
"""
from typing import List

import PIL.Image
import torch
import torchvision.models as models
from torchvision.transforms import Compose, Resize, CenterCrop, ToTensor, Normalize


class Encoder(object):

    def __init__(self):
        """
        Returns a new Encoder instance with attributes:
            model (models.vgg.VGG): pre-trained deep neural network with last classification layer removed.
            transform (Compose): input image transformation that the model expect.
        """
        self.model = models.vgg16(pretrained=True)
        self.model.classifier = torch.nn.Sequential(*[self.model.classifier[i] for i in range(5)])
        self.transform = Compose([
            Resize(255),
            CenterCrop(224),
            ToTensor(),
            Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])

    def encode(self, image_array: PIL.Image.Image) -> List[float]:
        """
        Encodes a RGB image into a fixed-sized vector.
        Args:
            image_array (PIL.Image.Image): RGB image data
        Returns: (List[float]) encoding vector with fixed size n=3.
        """
        image_tensor = self.transform(image_array)
        image_batch = image_tensor.unsqueeze(0)
        with torch.no_grad():
            image_encoding = self.model(image_batch)[0].tolist()[:3]
        return image_encoding



# MODELE

"""
This scripts computes a vector embedding for each image file stored on my AWS S3 storage, then writes a matrix
with all all vector embeddings along with image labels in a .csv file.
"""
import PIL.Image
import numpy as np
from pyspark.sql.types import Row

#from common import start_spark
#from encoder import Encoder



def load_image(img: Row) -> PIL.Image.Image:
    """
    Load an image from pyspark row.
    Args:
        img (Row): a row that contains the image to be loaded.
            It should have the attributes specified in `pyspark.ml.image.ImageSchema.columnSchema`.
    Returns: (PIL.Image.Image) RGB image data
    """
    image_array = np.ndarray(
        buffer=img.data,
        dtype=np.uint8,
        shape=(img.height, img.width, img.nChannels),
        strides=(img.width * img.nChannels, img.nChannels, 1)
    )
    image_array = image_array[:, :, ::-1]  # rotate colors from BGR to RGB

    return PIL.Image.fromarray(image_array)


def process_image(img: Row) -> Row:
    """
    Encodes an image into a fixed-length vector of float values using a pre-trained DNN encoder.
    Args:
        img (Row): a row that contains the image to be processed.
            It should have the attributes specified in `pyspark.ml.image.ImageSchema.columnSchema`.
    Returns: (Row) the image encoding, represented by a Row with fields:
        origin (str): image original path (i.e., the image key in S3 bucket)
        label (str): the image label
        x0 (float): first feature of image encoding
        (...)
        x_(n-1) (float): last feature of image encoding
    """
    print(f"...... process image {img.origin}")
    image_label = img.origin.split('/')[-2]
    image_array = load_image(img=img)
    image_encoding = my_encoder.value.encode(image_array)
    image_encoding = {f'x{i}': value for i, value in enumerate(image_encoding)}
    return Row(origin=img.origin, label=image_label, **image_encoding)


if __name__ == '__main__':
    spark = start_spark()

    print("... list images from Fruits 360 dataset")
    df = spark.read.format('Image').load(f's3a://nsaintgeoursbucket/*/*.jpg')

    print("... load image encoder")
    my_encoder = spark.sparkContext.broadcast(Encoder())

    print("... encode images and write output to file")
    output = df.select('image.*').rdd.map(process_image).toDF()
    output.coalesce(1).write.csv("output", header="true", mode="overwrite")