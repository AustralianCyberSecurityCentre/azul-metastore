# Test Data

All the test data in this directory are derived from arbitrary text made to hit certain edge cases.

Text files with various encodings to test for edge cases and odd encoding behaviour:

- 0ca0bafc3d3fd6960c7f7bc6c63064279746bab14ea7aaed735f43049c9627dc.txt
- 1d4ce380c90339830b7915137b34cd77c627ad498c773f9b4a12a3c876bfe024.txt
- b20f2ef76a6ebff096d5c75f703e4febd56e70fefc60e08b1692d2f50e364d8c.txt
- 189daa7d922f94a6fd9c0a9e5fe07d2058e4f1e48ea53dfe2a87a9bb40c1e96b.txt
- 9a018b074469a02814d956ee1a52b5ec835d09fd49638a5ebdf2f53feb988f2f.txt

Other files:

fake_meta.txt - random text in a file.
fake_meta.txt.malpz - malpz neutered copy of fake_meta.txt

fake.tar.gz - tar gzipped version of fake.txt
fake.zip - zipped version of fake.txt
fake.txt - random text in a file.
fake.txt.malpz - malpz neutered copy of fake.txt
fake.txt.cart - cart neutered copy of fake.txt
fake_infected.zip - fake.txt zipped with the password "infected".
aes_zip.zip - AES zipped copy of fake.txt.
zipped.cart.zip - zipped version of fake.txt.cart.

cartedzipfaketext.zip.cart - fake.txt zipped and then neutered with cart.
cartedzipfaketext.zip.malpz - fake.txt zipped and then neutered with malpz.
cartedzipfaketext.txt - copy of fake.txt.

harden_long_files.zip - zip file with lots of large text files inside it, all randomly generated text.
harden_too_many_files.zip - zip file with lots of sub files all with the word "test".
multi_sub_folder.zip - zip folder with multiple sub folders with random text in the subfolders files.
