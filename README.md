# 🛡️ Aegis Core v15.5: Motore di Governance Etica per IA

Aegis Core è un framework open-source progettato per rilevare e mitigare **bias algoritmici** e rischi etici nelle decisioni generate da sistemi di Intelligenza Artificiale in tempo reale.

Il sistema opera come un livello intermedio di validazione etica, analizzando lo stato decisionale (tramite un array NumPy) prima che le azioni vengano eseguite.

## 🎯 Obiettivi Chiave del Framework
* Rilevamento real-time di bias discriminatori
* Calcolo quantitativo di rischio etico (**IAEC**) e severità
* Architettura modulare e production-ready per l'integrazione

---

## 📄 Documentazione e Filosofia Etica

**ATTENZIONE:** La Licenza MIT non impone restrizioni, ma l'autore invita l'utente ad aderire volontariamente ai principi di AI Responsabile.

Per le specifiche tecniche, il processo di installazione e la forte filosofia etica del progetto, consultare il report completo:

* **[Report Tecnico Completo (PDF)](./Report%20tecnico%20Aegis%20Core%20v15.5%20.PDF)** *(**Nota:** il file deve essere caricato con questo nome esatto.)*

## ⚖️ Licenza
Aegis Core è rilasciato sotto [Licenza MIT](./LICENSE) per favorire la massima flessibilità d'uso.

---

## 💻 Installazione (Dipendenze Python 3.8+)
Il codice sorgente è fornito nel file `ethical_agent_v15_5.py`.

Installazione delle dipendenze necessarie:
```bash
pip install numpy fastapi pydantic httpx prometheus-client```

Versione archiviata su Zenodo: [10.5281/zenodo.17358074](https://doi.org/10.5281/zenodo.17358074)
