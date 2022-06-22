"""
This script uploads a sample of the Fruits 360 dataset from Github to AWS S3 storage.
Make sure you AWS credentials are available in a local file ~/.aws/credentials
"""
import random

import boto3
import requests
from tqdm import tqdm


def iterate_fruits_360_dataset(n_images: int = 1000):
    """
    Iterate over training images from Fruits 360 dataset on Github.
    Args:
        n_images (int or None): if not None, then the function only iterates over a random sample of 'n_images' images,
         otherwise the function iterates over the full dataset.
    Yields: (image_data, image_label, image_name) tuples with
        image_data (bytearray): RGB image as byte array
        image_label (str): image label, that is, a fruit variety
        img_name (str): image name, that is, original filename stem on Github repo
    """
    # Useful Github paths
    github_user, github_repo = 'Horea94', 'Fruit-Images-Dataset'
    github_list_tree_url = f"https://api.github.com/repos/{github_user}/{github_repo}/git/trees/master?recursive=1"
    github_raw_master_url = f'https://github.com/{github_user}/{github_repo}/raw/master'

    # List all training images in dataset
    res = requests.get(github_list_tree_url)
    images = [file['path'] for file in res.json()["tree"] if ('Training' in file['path'] and '.jpg' in file['path'])]

    # Yield training images
    images = random.sample(images, n_images) if n_images else images
    for image in images:
        image_label = image.split('/')[1].replace(' ', '')
        image_name = image.split('/')[-1]
        image_data = requests.get(f'{github_raw_master_url}/{image}', stream=True).raw

        yield image_data, image_label, image_name


if __name__ == '__main__':

    # Connect to S3 storage
    s3_bucket = boto3.resource('s3').Bucket('nsaintgeoursbucket')

    # Empty S3 storage
    s3_bucket.objects.all().delete()

    # Iterate over Fruits 360 dataset and upload training images to S3 storage
    for img_data, img_label, img_name in tqdm(iterate_fruits_360_dataset(n_images=1000)):
        s3_bucket.upload_fileobj(img_data, Key=f'{img_label}/{img_name}')