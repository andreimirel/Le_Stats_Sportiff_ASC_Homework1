# Birtia Andrei-Mirel, Grupa 331CB, Le Stats Sportiff - Tema1 ASC

## Explicatie pentru solutia aleasa

### In implementarea acestei teme, am dezvoltat un sistem de analiza a datelor nutritionale si de obezitate care utilizeaza procesarea asincrona bazata pe un model de thread pool. Arhitectura sistemului este structurata in trei componente functionale principale:
  #### DataIngestor - Componenta responsabila pentru incarcarea si procesarea datelor din fisierele CSV, realizarea calculelor statistice si filtrarea datelor. Aceasta implementeaza diverse metode de analiza precum calculul mediilor pe state, identificarea statelor cu cele mai bune/slabe performante si calculul diferentelor fata de media globala.
  #### ThreadPool si TaskRunner - Implementarea sistemului de procesare paralela a task-urilor. ThreadPool initializeaza si gestioneaza un numar configurat de fire de executie, in timp ce TaskRunner executa efectiv job-urile din coada de asteptare. Aceasta abordare permite procesarea concurenta a mai multor cereri.
  #### Routes (API) - Expune functionalitatile sistemului prin intermediul unei interfete API RESTful, oferind endpoint-uri pentru diferite tipuri de analize si operatiuni de gestionare a job-urilor.

### In clasa ThreadPool, am initializat structurile de date necesare si am pornit thread-urile pentru rezolvarea task-urilor. Am implementat doua functii principale:
    add_job - adauga un job in coada de asteptare si actualizeaza statusul acestuia in "running"
    shutdown - initiaza inchiderea ordonata a sistemului, permitand finalizarea job-urilor in curs

### In clasa TaskRunner, metoda run extrage secvential job-urile din coada, le executa si inregistreaza rezultatele. Dupa executie, statusul job-ului este actualizat in "done", iar in caz de eroare in "error". Rezultatele sunt salvate atat in memoria sistemului, cat si in fisiere individuale in directorul "results/".

### Pentru procesarea datelor , am incarcat fisierul CSV folosind biblioteca pandas intr-o variabila dataset. Functiile de analiza folosesc abordari similare:
    filtrarea datelor in functie de intrebarea specificata, intervalul de ani (2011-2022) si, cand este cazul, statul specificat
    utilizarea functiei groupby pentru a grupa datele pe state
    calculul mediilor folosind functia mean
    transformarea rezultatelor in format dictionar/JSON pentru a fi returnate

### Pentru functiile best5 si worst5, am verificat daca intrebarea face parte din categoria celor in care valorile minime sau maxime sunt considerate optime, iar in functie de aceasta, am returnat fie primele 5 elemente, fie ultimele 5 din rezultatele sortate.

### Consider ca tema este deosebit de utila pentru ca:
    ofera o intelegere aprofundata a mecanismelor de procesare concurenta in context real
    familiarizeaza cu tehnici de analiza a datelor statistice
    implementeaza un API REST functional pentru interogarea datelor
    dezvolta abilitati de gestionare a resurselor partajate si sincronizare

### Implementare
    toate operatiile de analiza a datelor nutritionale si de obezitate
    procesarea paralela a job-urilor folosind thread pool
    API RESTful pentru interogarea datelor

### Functionalitati suplimentare:
    mecanism de "graceful shutdown" care permite serverului sa termine procesarea job-urilor in curs inainte de oprire
    persistenta rezultatelor pe disc in format JSON pentru recuperare in caz de erori
    sistem de logging extins pentru depanare si monitorizare
    endpoint-uri aditionale pentru monitorizarea starii job-urilor

### Dificultati intampinate:
    gestionarea corecta a sincronizarii intre thread-uri pentru a preveni conditiile de concurenta
    implementarea unui mecanism robust de tratare a erorilor pentru job-urile care esueaza
    optimizarea performantei pentru operatii complexe de filtrare si agregare a datelor

### Lucruri interesante descoperite:
    eficienta bibliotecii pandas pentru manipularea si analiza seturilor mari de date
    implementarea pattern-ului producer-consumer folosind Queue din Python
    tehnici de logging si debugging pentru aplicatii multi-thread

### Git
    link catre repository: [Link](https://github.com/andreimirel/Le_Stats_Sportiff_ASC_Homework1)