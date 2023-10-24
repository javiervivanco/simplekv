#lang racket/base
(require simple-kv racket/string)
;;(define-kv-origin "/home/usuario/data")
(define metrics (make-hash '((set! . ())
                             (ref . ())
                             (del! . ()))))
(define (metric-set! key val)
  (hash-set! metrics key (append (hash-ref metrics key ) (list val))))

(define (benchmark num-threads num-ops-per-thread)

  (define (worker)
    (for ([i (in-range num-ops-per-thread)])
      (define key (gensym))
      
      ;; Medir el tiempo de kv-set!
      (define start-time-set (current-inexact-milliseconds))
      (kv-set! key  "val")
      (define end-time-set (current-inexact-milliseconds))
      
      ;; Medir el tiempo de kv-ref
      (define start-time-ref (current-inexact-milliseconds))
      (kv-ref key)
      (define end-time-ref (current-inexact-milliseconds))
      
      ;; Medir el tiempo de kv-del!
      (define start-time-del (current-inexact-milliseconds))
      (kv-del! key)
      (define end-time-del (current-inexact-milliseconds))
      
      ;; Imprimir los tiempos
      (metric-set! 'set! (- end-time-set start-time-set))
      (metric-set! 'ref  (- end-time-ref start-time-ref))
      (metric-set! 'del! (- end-time-del start-time-del))
      )
    (show-metric 'set!)
    (show-metric 'ref)
    (show-metric 'del!))
  (for ([i (in-range num-threads)])
    (thread worker)))
  


(define (show-metric key)
  (displayln (format "~a :~ams "key (/ (apply + (hash-ref metrics key)) (length (hash-ref metrics key))))) )


(benchmark 10 10) ;; 10 threads, 10 operaciones por thread
(benchmark 10 100) ;; 10 threads, 100 operaciones por thread
(benchmark 10 1000) ;; 10 threads, 1000 operaciones por thread
