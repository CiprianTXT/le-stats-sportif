# Tema 1 ASC - Le Stats Sportif

În cadrul acestei teme am avut de implementat un server în Python, folosind framework-ul Flask, care poate gestiona cereri HTTP, pornind de la un set de date în format CSV (Comma-separated values). În urma acestor request-uri, serverul prelucrează datele din CSV obținând statistici pe care le oferă ulterior clientului.
De asemenea, pentru a testa corectitudinea rezultatelor oferite de server, am mai avut de implementat un script de Unittest, tot în Python.

## Implementare
### app/
- ### \_\_init\_\_.py:

    Am inițializat și configurat logger-ul astfel:
    - fișierele de log vor fi stocate într-un folder numit `logs`, pe care îl creez în cazul în care acesta nu există;
    - dimensiunea maximă a unui fișier de log va fi de 1 MB;
    - numărul maxim de fișiere de log va fi de 10;
    - fișierele vor fi encodate UTF-8;
    - am setat nivel de logging pe INFO.

    Ulterior, am verificat ca folder-ul `results`, după care am ales numărul de thread-uri pe care serverul îl va avea în funcție de variabila de mediu `TP_NUM_OF_THREADS`.

    În final, am pornit serverul de Flask, împreună cu DataIngestor-ul și cu TaskPool-ul (pe care le-am logat). Job counter-ul a fost inițializat cu valoarea 1.

- ### data_ingestor.py:

    Conține clasa DataIngestor, care se ocupa cu citirea datelor din CSV. Pentru asta am folosit funcția `read_csv()` din modulul `pandas`. Tot în cadrul acestei clase sunt definite întrebările din cadrul tabelului citit anterior, categorisite în funcție de valorile cele mai bune.

- ### routes.py:

    Aici sunt definite rutele HTTP folosite de server.
    1. /api/num_jobs

        Verifică dicționarul din thread pool care stochează status-ul fiecărui job și returnează un răspuns JSON care conține numărul de joburi în execuție la acel moment de timp.

    2. /api/jobs

        Iterează prin dicționarul din thread pool și creează o listă cu status-ul fiecărui job înregistrat până în acel moment de timp.

    3. /api/get_results/<job_id>

        Accesează fișierul JSON rezultat în urma execuției job-ului cerut în request. Dacă `job_id` nu se află în intervalul [1, job_counter), atunci acesta este invalid, fiind trimis spre client un răspuns de eroare. Dacă în urma verificării dicționarului, job-ul este marcat ca "running", atunci se returnează un mesaj cu status-ul "running".

    4. /api/states_mean

        Adaugă în coada de execuție un job de tipul `states_mean` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    5. /api/state_mean

        Adaugă în coada de execuție un job de tipul `state_mean` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    6. /api/best5

        Adaugă în coada de execuție un job de tipul `best5` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    7. /api/worst5

        Adaugă în coada de execuție un job de tipul `worst5` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    8. /api/global_mean

        Adaugă în coada de execuție un job de tipul `global_mean` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    9. /api/diff_from_mean

        Adaugă în coada de execuție un job de tipul `diff_from_mean` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    10. /api/state_diff_from_mean

        Adaugă în coada de execuție un job de tipul `state_diff_from_mean` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    11. /api/mean_by_category

        Adaugă în coada de execuție un job de tipul `mean_by_category` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    12. /api/state_mean_by_category

        Adaugă în coada de execuție un job de tipul `state_mean_by_category` doar dacă thread pool-ul nu a fost oprit. Structura job-ului adăugat este o listă care are pe prima poziție tipul de request, pe a doua poziție are query-ul primit de la client, iar pe a treia poziție are `job_id-ul` alocat. Adăugarea job-ului este urmată de trezirea unui thread care așteaptă sarcini de executat, folosind `condition.notify()`. În final, este returnat clientului un răspuns JSON cu `job_id-ul` alocat cererii lui.

    13. /api/graceful_shutdown

        Oprește thread pool-ul prin apelarea metodei `shutdown()` din clasa ThreadPool și trezește toate thread-urile care așteaptă sarcini noi folosind `condition.notify_all()`, astfel încât acestea se vor opri în lispa unor job-uri în coada de execuție. Se va returna, în cele din urmă, un răspuns JSON care atenționează clientul că serverul se oprește.

    După cum se poate observa, rutele 4-12 funcționează similar, aproape identic. Astfel, am definit decoratorul `request_handler()` care primește tipul de request și execută pașii descriși mai sus.

- ### task_runner.py:

    Conține 2 clase: ThreadPool și TaskRunner.

    În ThreadPool primesc numărul de thread-uri de creeat, o instanță a clasei DataIngestor cu datele citite din CSV și obiectul de log cu care înregistrez parcursul execuțiilor din program.

    Aici îmi inițializez coada de execuție a sarcinilor, dicționarul cu status-ul job-urilor înregistrate în thread pool, lista pe care o folosesc pentru a înregistra un shut down event (fiind un obiect mutabil este ușor de partajat cu toate thread-urile) și instanța clasei Condition.

    Pentru lizibilitate, am definit 2 metode: `is_running()` și `shutdown()`. Prima metodă verifică dacă lista asociată event-ului de shut down conține elemente, iar a doua adaugă în listă un element (imediat vizibil pentru toate thread-urile).

    În TaskRunner primesc coada de job-uri, dicționarul cu status-ul job-urilor, notificarea de shutdown, instanța clasei Condition, instanța clasei DataIngestor cu datele citite din CSV și obiectul de log cu care înregistrez parcursul execuțiilor din program. Această clasă conține toate rutinele de execuție folosite pentru a prelucra datele din tabel în funcție de query-ul primit de la client.
    1. run()

        Aceasta este rutina care va fi executată de thread-uri cât timp nu s-a înregistrat vreo notificare de shutdown sau coada de execuție nu este goală. Dacă coada este goală, thread-urile vor intra în așteptare, altfel se scoate din coadă un job, care va declanșa în funcție de tipul acestuia rutina de execuție specifică request-ului. La finalul procesării unui job, thread-ul îl va marca ca și "done", iar rezultatul scris pe disc poate fi accesat ulterior la nevoie.

    2. save_job_to_disk()

        Metodă auxiliară care primește un `result` și un `job_id`, astfel încât aceasta va scrie pe disc în folder-ul `results` rezultatul procesării într-un fișier JSON numit `job_id_<job_id>.json`.

    3. exec_states_mean()

        Primește întrebarea clientului și un job_id. Se filtrează tabelul în funcție de întrebare și se grupează apoi după coloana de state (`LocationDesc`). Prin tabelul rezultat, se va itera fiecare stat, după care se adaugă într-o listă un tuplu de forma (stat, media valorilor din coloana `Data_Value`). În final, se realizează o sortare crescătoare a listei după a doua valoare din tuplu, folosind o funcție lambda, după care se realizează o conversie la dicționar a listei, obținându-se astfel rezultatul procesării de salvat pe disc.

    4. exec_state_mean()

        Primește întrebarea clientului, statul dorit și un job_id. Se filtrează tabelul în funcție de întrebare și de stat. Din tabelul rezultat, se va extrage media valorilor din `Data_Value` care va fi adăugat într-un dicționar, obținându-se astfel rezultatul procesării de salvat pe disc.

    5. exec_top5()

        Funcționează similar cu `states_mean`, însă rezultatul procesării este influențat de parametrul `best`. Dacă acesta este setat pe `True`, în momentul sortării se verifică categoria din care face parte întrebarea și sunt extrase primele 5 valori considerate cele mai bune. Rezultatul este stocat într-un dicționar care va fi ulterior salvat pe disc.

    6. exec_global_mean()

        Primește o întrebare și un job_id. Se realizează o filtrare după întrebare, după care media valorilor din coloana `Data_Value` este stocată într-un dicționar, obținându-se astfel rezultatul procesării de salvat pe disc.

    7. exec_diff_from_mean()

        Primește întrebarea clientului și un job_id. Se filtrează tabelul în funcție de întrebare de unde se obține media globală a valorilor. Apoi, tabelul se grupează după coloana de state (`LocationDesc`). Prin tabelul rezultat, se va itera fiecare stat, după care se adaugă într-o listă un tuplu de forma (stat, diferența dintre media globală și media valorilor din coloana `Data_Value`). În final, se realizează o sortare crescătoare a listei după a doua valoare din tuplu, folosind o funcție lambda, după care se realizează o conversie la dicționar a listei, obținându-se astfel rezultatul procesării de salvat pe disc.

    8. exec_state_diff_from_mean()

        Similar cu `exec_diff_from_mean()`. Diferă prin faptul că metoda mai primește un stat pentru care se dorește diferența dintre media globală și media valorilor aferente lui. Față de metoda de mai sus, nu se mai grupează după coloana de state, ci se face direct o filtrare după întrebare și stat. Rezultatul este mai apoi salvat pe disc.

    9. exec_mean_by_category()

        Primește o întrebare și un job_id. Tabelul este filtrat după întrebare, iar apoi este grupat după `LocationDesc` (state), `StratificationCategory1` (categorie) și `Stratification1`. Prin tabelul rezultat, se va itera fiecare categorie, după care media valorilor din `Data_Value` se adaugă într-un dicționar. Rezultatul procesării este mai apoi salvat pe disc.

    10. exec_state_mean_by_category()

        Similar cu `exec_mean_by_category()`. Diferă prin faptul că metoda mai primește un stat ca parametru. Față de metoda de mai sus, gruparea nu se mai face și după stat, în schimb statul dorit este adăugat ca și filtru. Rezultatul este apoi salvat pe disc.

### unittests/
- ### references/

    Acest folder conține toate cele 9 rezultate de referință, câte un rezultat pentru fiecare rutină de execuție din clasa TaskRunner.

- ### test_routines.py:

    Conține clasa TestExecRoutines, unde sunt definite 9 teste pentru cele 8 metode din clasa TaskRunner. La începutul fiecărui test creez folder-ul `results` și inițializez o instanță a clasei TaskRunner, fără coadă de execuție, dicționar de stare a job-urilor, elemente de sincronizare a thread-urilor și obiect de log, doar instanța clasei DataIngestor pentru citirea datelor din CSV, astfel îmi creez mediul optim de testare a rutinelor de execuție. La finalul fiecărui test, șterg folder-ul `results`.
