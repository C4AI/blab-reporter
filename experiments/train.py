__author__='thiagocastroferreira'

import torch
from bleurt_pytorch import BleurtConfig, BleurtForSequenceClassification, BleurtTokenizer

import json
import argparse
from models.bartgen import BARTGen

from models.gportuguesegen import GPorTugueseGen
from models.t5gen import T5Gen
from models.gpt2 import GPT2
from models.blenderbot import Blenderbot
from torch.utils.data import DataLoader, Dataset
import nltk
from nltk.translate.bleu_score import corpus_bleu, SmoothingFunction
import nltk.translate.gleu_score as gleu
from nltk.translate import meteor
import numpy as np
import os
import torch
from torch import optim
from rouge import Rouge
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('omw-1.4')


def wer_score(hyp, ref, print_matrix=False):
  N = len(hyp)
  M = len(ref)
  L = np.zeros((N,M))
  for i in range(0, N):
    for j in range(0, M):
      if min(i,j) == 0:
        L[i,j] = max(i,j)
      else:
        deletion = L[i-1,j] + 1
        insertion = L[i,j-1] + 1
        sub = 1 if hyp[i] != ref[j] else 0
        substitution = L[i-1,j-1] + sub
        L[i,j] = min(deletion, min(insertion, substitution))
        # print("{} - {}: del {} ins {} sub {} s {}".format(hyp[i], ref[j], deletion, insertion, substitution, sub))
  if print_matrix:
    print("WER matrix ({}x{}): ".format(N, M))
    print(L)
  return int(L[N-1, M-1])



class Trainer:
    '''
    Module for training a generative neural model
    '''
    def __init__(self, model, trainloader, devdata, optimizer, epochs, \
        batch_status, device, write_path, early_stop=5, verbose=True, language='english'):
        '''
        params:
        ---
            model: model to be trained
            trainloader: training data
            devdata: dev data
            optimizer
            epochs: number of epochs
            batch_status: update the loss after each 'batch_status' updates
            device: cpu or gpy
            write_path: folder to save best model
            early_stop
            verbose
            language
        '''
        self.model = model
        self.optimizer = optimizer
        self.epochs = epochs
        self.batch_status = batch_status
        self.device = device
        self.early_stop = early_stop
        self.verbose = verbose
        self.trainloader = trainloader
        self.devdata = devdata
        self.write_path = write_path
        self.language = language
        if not os.path.exists(write_path):
            os.mkdir(write_path)
    
    def train(self):
        '''
        Train model based on the parameters specified in __init__ function
        '''
        max_bleu, repeat = 0, 0
        for epoch in range(self.epochs):
            self.model.model.train()
            losses = []
            for batch_idx, inp in enumerate(self.trainloader):
                intents, texts = inp['X'], inp['y']
                self.optimizer.zero_grad()

                # generating
                output = self.model(intents, texts)

                # Calculate loss
                loss = output.loss
                losses.append(float(loss))

                # Backpropagation
                loss.backward()
                self.optimizer.step()

                # Display
                if (batch_idx+1) % self.batch_status == 0:
                    print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}\tTotal Loss: {:.6f}'.format(epoch, \
                        batch_idx+1, len(self.trainloader), 100. * batch_idx / len(self.trainloader), 
                        float(loss), round(sum(losses) / len(losses), 5)))
            
            # metrics
            bleu_score, rouge_score, meteor_score, gleu_score, bleurt_score, comet_score, prism_score, acc = self.evaluate()
            
            print ('Results')
            print ('*************')
            print ('BLEU:', bleu_score)
            print ('ROUGE:', rouge_score)
            print ('METEOR:', meteor_score)
            print ('GLEU:', gleu_score)
            print ('BLEURT:', bleurt_score)
            print ('COMET:', comet_score)
            print ('PRISM:', prism_score)
            print ('ACC:', acc)
            print ('*************')
            
            checkpoint = { 'epoch': epoch+1, 'bleu': bleu, 'acc': acc, 'best_model': False }
            if bleu > max_bleu:
                self.model.model.save_pretrained(os.path.join(self.write_path, 'model'))
                max_bleu = bleu
                repeat = 0
                checkpoint['best_model'] = True
                print('Saving best model...')
            else:
                repeat += 1
            
            if repeat == self.early_stop:
                break
            
            # saving checkpoint
            if os.path.exists(f"{self.write_path}/checkpoint.json"):
                checkpoints = json.load(open(f"{self.write_path}/checkpoint.json"))
                checkpoints['checkpoints'].append(checkpoint)
            else:
                checkpoints = { 'checkpoints': [checkpoint] }
            json.dump(checkpoints, open(f"{self.write_path}/checkpoint.json", 'w'), separators=(',', ':'), sort_keys=True, indent=4)
    
    def evaluate(self):
        '''
        Evaluating the model in devset after each epoch
        '''
    
        self.model.model.eval()
        results = {}
        for batch_idx, inp in enumerate(self.devdata):
            intent, text = inp['X'], inp['y']
            if intent not in results:
                results[intent] = { 'hyp': '', 'refs': [] }
                # predict
                output = self.model([intent])
                results[intent]['hyp'] = output[0]

                # Display
                if (batch_idx+1) % self.batch_status == 0:
                    print('Evaluation: [{}/{} ({:.0f}%)]'.format(batch_idx+1, \
                        len(self.devdata), 100. * batch_idx / len(self.devdata)))
            
            results[intent]['refs'].append(text)
        
        hyps, refs, acc = [], [], 0
        for i, intent in enumerate(results.keys()):
            if i < 50 and self.verbose:
                print('Real: ', results[intent]['refs'][0])
                print('Pred: ', results[intent]['hyp'])
                print()
            
            if self.language != 'english':
                hyps.append(nltk.word_tokenize(results[intent]['hyp'], language=self.language))
                refs.append([nltk.word_tokenize(ref, language=self.language) for ref in results[intent]['refs']])
            else:
                hyps.append(nltk.word_tokenize(results[intent]['hyp']))
                refs.append([nltk.word_tokenize(ref) for ref in results[intent]['refs']])
            
            if results[intent]['hyp'] in results[intent]['refs'][0]:
                acc += 1
        
        chencherry = SmoothingFunction()
        
        # bleu
        bleu_score = corpus_bleu(refs, hyps, smoothing_function=chencherry.method3)
        
        # gleu
        gleu_score = gleu.corpus_gleu(refs, hyps)
        
        # wer
        lista_wer = []
        for valor1, valor2 in zip(hyps, refs):
          lista_wer.append(wer_score(valor1, valor2, print_matrix=False))
        werscore = np.mean(lista_wer)

        # rouge
        rouge = Rouge()
        lista_rogue = []
        for valor1, valor2 in zip(hyps, refs[0]):
          tmp = rouge.get_scores(' '.join(valor1), ' '.join(valor2))
          lista_rogue.append(tmp[0]['rouge-1']['f'])
        rouge_score = np.mean(lista_rogue)
        
        # meteor
        lista_meteor = []
        for valor1, valor2 in zip(hyps, refs[0]):
          lista_meteor.append(meteor([valor1], valor2))
        meteor_score = np.mean(lista_meteor)      

        # bleurt
        config = BleurtConfig.from_pretrained('lucadiliello/BLEURT-20')
        model = BleurtForSequenceClassification.from_pretrained('lucadiliello/BLEURT-20')
        tokenizer = BleurtTokenizer.from_pretrained('lucadiliello/BLEURT-20')
        
        model.eval()
        lista_bleurt = []
        print (refs)
        print (hyps)
        for valor1, valor2 in zip(refs, hyps):
          with torch.no_grad():
              inputs = tokenizer(' '.join(valor1[0]), ' '.join(valor2), padding='longest', return_tensors='pt')
              res = model(**inputs).logits.flatten().tolist()
          lista_bleurt.append(res[0])
        bleurt_score = np.mean(lista_bleurt)

        # comet
        comet_score = 1
        
        # prism
        prism_score = 1

        # acc
        acc = float(acc) / len(results)
        
        return bleu_score, rouge_score, meteor_score, gleu_score, bleurt_score, comet_score, prism_score, acc

class NewsDataset(Dataset):
    def __init__(self, data):
        """
        Args:
            data (string): data
        """
        self.data = data

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return self.data[idx]

    
def load_data(src_fname, trg_fname):
    with open(src_fname) as f:
        src = f.read().split('\n')
    with open(trg_fname) as f:
        trg = f.read().split('\n') 
    
    assert len(src) == len(trg)
    data = [{ 'X': src[i], 'y': trg[i] } for i in range(len(src))]
    return data
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--tokenizer", help="path to the tokenizer", required=True)
    parser.add_argument("--model", help="path to the model", required=True)
    parser.add_argument("--src_train", help="path to the source train data", required=True)
    parser.add_argument("--trg_train", help="path to the target train data", required=True)
    parser.add_argument("--src_dev", help="path to the source dev data", required=True)
    parser.add_argument("--trg_dev", help="path to the target dev data", required=True)
    parser.add_argument("--epochs", help="number of epochs", type=int, default=5)
    parser.add_argument("--learning_rate", help="learning rate", type=float, default=1e-5)
    parser.add_argument("--batch_size", help="batch size", type=int, default=16)
    parser.add_argument("--early_stop", help="earling stop", type=int, default=3)
    parser.add_argument("--max_length", help="maximum length to be processed by the network", type=int, default=180)
    parser.add_argument("--write_path", help="path to write best model", required=True)
    parser.add_argument("--language", help="language", default='english')
    parser.add_argument("--verbose", help="should display the loss?", action="store_true")
    parser.add_argument("--batch_status", help="display of loss", type=int)
    parser.add_argument("--cuda", help="use CUDA", action="store_true")
    parser.add_argument("--src_lang", help="source language of mBART tokenizer", default='pt_XX')
    parser.add_argument("--trg_lang", help="target language of mBART tokenizer", default='pt_XX')
    
    # settings
    args = parser.parse_args()
    learning_rate = args.learning_rate
    epochs = args.epochs
    batch_size = args.batch_size
    batch_status = args.batch_status
    early_stop =args.early_stop
    language = args.language
    try:
        verbose = args.verbose
    except:
        verbose = False
    try:
        device = 'cuda' if args.cuda else 'cpu'
    except:
        device = 'cpu'
    write_path = args.write_path

    # model
    max_length = args.max_length
    tokenizer_path = args.tokenizer
    model_path = args.model
    if 'mbart' in tokenizer_path:
        src_lang = args.src_lang
        trg_lang = args.trg_lang
        generator = BARTGen(tokenizer_path, model_path, max_length, device, True, src_lang, trg_lang)
    elif 'bart' in tokenizer_path:
        generator = BARTGen(tokenizer_path, model_path, max_length, device, False)
    elif 'bert' in tokenizer_path:
        generator = BERTGen(tokenizer_path, model_path, max_length, device)
    elif 'mt5' in tokenizer_path:
        generator = T5Gen(tokenizer_path, model_path, max_length, device, True)
    elif 't5' in tokenizer_path:
        generator = T5Gen(tokenizer_path, model_path, max_length, device, False)
    elif 'gpt2' in tokenizer_path:
        generator = GPorTugueseGen(tokenizer_path, model_path, max_length, device)
    elif tokenizer_path == 'gpt2':
        generator = GPT2(tokenizer_path, model_path, max_length, device)
    elif 'blenderbot' in tokenizer_path:
        generator = Blenderbot(tokenizer_path, model_path, max_length, device)
    else:
        raise Exception("Invalid model") 

    # train data
    src_fname = args.src_train
    trg_fname = args.trg_train
    data = load_data(src_fname, trg_fname)
    dataset = NewsDataset(data)
    trainloader = DataLoader(dataset, batch_size=batch_size)
    
    # dev data
    src_fname = args.src_dev
    trg_fname = args.trg_dev
    devdata = load_data(src_fname, trg_fname)

    # optimizer
    optimizer = optim.AdamW(generator.model.parameters(), lr=learning_rate)
    
    # trainer
    trainer = Trainer(generator, trainloader, devdata, optimizer, epochs, batch_status, device, write_path, early_stop, verbose, language)
    trainer.train()
