# Document-Processing-using-Map-Reduce-Paradigm

<h5>Calcan Elena-Claudia <br/>
331CA</h5><br/>


   • este implementat un program care proceseaza un set de documente text, 
    folosind modelul Map-Reduce
    
   • thread-ul coordonator primeste dimensiunea frgmentelor si documentele
    ce vor fi procesate
    
   • la inceput coordonatorul calculeaza pentru fiecare document un offset
    care indica inceputul fragmentelor din fiecare document
   
   • coordonatorul mai intai lanseaza task-uri de tip Map, urmand sa lanseze
    task-urile de tip Reduce care vor prelucra solutiile rezultate de operatia
    anterioara
   
   • dupa terminarea operatiei de Reduce coordonatorul preia rezultatele finale
    si le scrie in fisierul de output in ordinea descrescatoare a rangurilor
    calculate

<br/>
    
## Map Task
-----------------------

   • in implementarea operatiei de Map s-a folosit Modelul Replicated Workers,
    mai precis s-a folosit CompletableFuture cu ExecutorService, pentru 
    salvarea rezultatelor partiale
   
   • in thread-ul coordonator s-a instantiat cate un ExecutorService cu un numar 
    de workeri egal cu numarul de thread-uri, dat ca parametru in linia de comanda
   
   • task-urile care sunt trimise in poll reprezinta fragmentele ce trebuie analizate
   
   • ExecutorService isi termina executia atunci cand s-a ajuns la finalul fisierului
    
   
   • un worker realizeaza task-urile astfel:

        * ia offset-ul care indica inceputul fragmentului pe care il analizeaza
        * verifica daca fragmentul incepe in mijlocul unui cuvant sau nu si in functie
        de caz offset-ul se schimba, se duce pe urmatoarele pozitii din fisier pana da 
        de un delimitator, sau ramane neschimbat
        * pentru fiacre cuvant din fragment se incrementeaza numarul de aparitii care 
        au aceeasi lungime
        * se updateaza lista de cuvinte de lungime maximala, cand este nevoie 

<br/>

## Reduce Task
-----------------------

   • thtread-ul coordonator preia solutiile partiale rezultate de task-ul de Map si le
    asigneaza workeri-lor ce executa task-ul de Reduce
    
   • fiecarui worker ii este asignat cate un document cu rezultatele sale partiale si 
    le combina
    
    
   • un worker realizeaza task-ul astfel:

        1. Etapa de combinare

            * preia dictionarele ce retin lungimile cuvintelor din document cu frcventa lor
              si le combina intr-un dictionar ce contine lungimile cuvintelor cu numarul lor
              de aparitii din document
            * preia listele cu cele mai lungi cuvinte din fragmente si salveaza cuvintele de
              lungime maxima din tot documentul
            
        2. Etapa de procesare
            
            * se calculeaza rangul documentului folosind dictionarul din etapa de combinare
            si sirul lui Fibonacci
            
