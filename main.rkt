#lang racket/base
(require racket/file racket/path racket/string racket/system)

(define max-delay (make-parameter 5))
(define k-delay (make-parameter 0.1))
(provide kv-origin kv-ref kv-set! kv-del! kv-ref! kv-key?  kv-keys kv-incr!  kv-append! k k! K! k? kv-remove! q-pop q-push key->path)

(define kv-origin (make-parameter #f))

(define-syntax-rule (k ID BODY ...)
  (kv-ref 'ID BODY ...))

(define-syntax-rule (k! ID BODY ...)
  (kv-ref! 'ID BODY ...))

(define-syntax-rule (K! ID VAL)
  (kv-set! 'ID VAL))

(define-syntax-rule (k? ID )
  (kv-key? 'ID ))

(define (kv-origin-check! )
  (unless (kv-origin) (error "undefined kv-origin " ))
  (unless (directory-exists? (kv-origin)) (make-directory* (kv-origin))))


(define (key->path key)
  (build-path (kv-origin) (string-append (symbol->string key) ".rktd")))

(define (key->lock key)
  (build-path (kv-origin) (string-append (symbol->string key) ".LOCK")))


(define (kv-exclusive key process)
  (kv-origin-check!)
  (define apath (key->path key))
  (call-with-file-lock/timeout
   #:max-delay (max-delay)
   #:delay (k-delay)
   #:lock-file (key->lock key)
   apath
   'exclusive
   process
   (lambda () (error "Failed to obtain lock for file" apath))))

(define (kv-shared key process)
  (kv-origin-check!)
  (define apath (key->path key))
  (call-with-file-lock/timeout
   #:max-delay (max-delay)
   #:delay (k-delay)
   #:lock-file (key->lock key)
   apath
   'shared
   process
   (lambda () (error "Failed to obtain lock for file" apath))))

(define (kv-value-write key val)
  (write-to-file val (key->path key) #:exists 'replace ))

(define (kv-value-read key)
  (let([value (file->value (key->path key))]) value))

(define (kv-value-ref key default)
  (cond
    [(kv-key? key) (kv-value-read key )]
    [else         default]))


(define (kv-key? key)
  (file-exists? (key->path key)))

(define (kv-keys )
  (kv-origin-check!)
  (define (data? a-path)
    (equal? (path-get-extension a-path) #".rktd"))
  (define (file->key a-path)
    (map string->symbol  (string-split (path->string a-path) ".rktd")))
  (map car (map file->key (filter  data? (directory-list (kv-origin))) )))


(define (kv-set! key val)
  (kv-exclusive key (lambda () (kv-value-write  key val))))

(define (kv-ref key [default (lambda () (error "no value found for key: " key))])
  (cond [(kv-key? key)         (kv-shared key (lambda () (kv-value-read key)))]
        [(procedure? default) (default)]
        [else                  default]))


(define (kv-ref! key to-set)
  (kv-exclusive key
   (λ ()
     (cond
       [(kv-key? key) (kv-value-read key)]
       [else         (kv-value-write key to-set)
                     to-set]))))

 
(define (kv-del! key)
  (kv-exclusive key (lambda () (delete-file  (key->path key)))))



(define (kv-incr! key)
  (kv-exclusive key (λ () (kv-value-write key (add1 (kv-value-ref key -1))))))
  


(define (kv-append! key . args)
  (kv-exclusive key  (λ () (kv-value-write key (append (kv-value-ref key '()) args )))))

(define (kv-member key val)
  (kv-shared key (λ () (member val (kv-value-ref key '())))))


(define (kv-remove! key id)
  (kv-exclusive key  (λ () (kv-value-write key (remove id (kv-value-ref key '()) )))))
  


(define (q-push queue value)
  (unless (kv-member '~queues queue) (kv-append! '~queues  queue))
  (kv-append! queue value)
  (void))

(define (q-pop queue-name)
  (kv-exclusive queue-name
   (λ ()
     (define queue (kv-value-ref queue-name '()))
     (if (null? queue)
         (void)
         (begin
           (kv-value-write queue-name  (cdr queue))
           (car queue))))))

(define (q-pending queue)
  (kv-ref! queue '()))


;:(define (q-consumed 'queue)

(module+ test
  (require rackunit)
  (kv-origin  (make-temporary-directory))
  (kv-set! 'key "val")
  (check-true  (file-exists? (build-path (kv-origin) "key.rktd") ))
  (define v1 "val")
  (kv-set! 'key1 v1)
  (check-equal? v1 (kv-ref 'key1))
  (check-true  (file-exists? (build-path (kv-origin) "key1.rktd") ))
  (check-true  (list? (member 'key (kv-keys) )))
  (check-false  (member 'keydsds (kv-keys) ))
  (kv-set! 'val-hash (make-hash '((cu . 10))))

  (kv-del! 'key)
  (kv-del! 'key1)
  (q-push 'queue 1)
  (q-pop 'queue )
  ;;(q-all 'queue)
  (q-pending 'queue)
  ;;(q-consumed 'queue)
  )
