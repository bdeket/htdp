#lang racket/base

(require racket/class
         racket/list
         racket/bool
         racket/match
         htdp/error
         racket/tcp)

(provide (all-defined-out))

(define INSET  5)     ;; the space around the image in the canvas
(define RATE   1/30)   ;; the clock tick rate 
(define TRIES  3)     ;; how many times should register try to connect to the server 
(define PAUSE  1/2)     ;; # secs to wait between attempts to connect to server 
(define SQPORT 4567) ;; the port on which universe traffic flows

;                                                                               
;                                                                               
;                                                                               
;    ;;;                                              ;;;                       
;   ;   ;                                            ;   ;                      
;   ;   ;                                            ;   ;                      
;   ;       ;;;   ;;;;;  ;;;;;   ;;;   ;;;;          ;   ;  ;   ;  ;   ;        
;   ;      ;   ;  ; ; ;  ; ; ;  ;   ;  ;   ;         ;;;;;  ;   ;   ; ;         
;   ;      ;   ;  ; ; ;  ; ; ;  ;   ;  ;   ;         ;   ;  ;   ;    ;          
;   ;      ;   ;  ; ; ;  ; ; ;  ;   ;  ;   ;         ;   ;  ;   ;    ;          
;   ;   ;  ;   ;  ; ; ;  ; ; ;  ;   ;  ;   ;         ;   ;  ;  ;;   ; ;    ;;   
;    ;;;    ;;;   ; ; ;  ; ; ;   ;;;   ;   ;         ;   ;   ;; ;  ;   ;   ;;   
;                                                                               
;                                                                               
;                                                                               

;; ---------------------------------------------------------------------------------------------------

;; Any -> Boolean
(define (nat? x)
  (and (number? x) (integer? x) (>= x 0)))

;; Number Symbol Symbol -> Integer
(define (number->integer x [t ""] [p ""])
  (check-arg t (and (number? x) (real? x)) "real number" p x)
  (inexact->exact (floor x)))

;; ---------------------------------------------------------------------------------------------------
;; Nat Nat ->String 
;; converts i to a string, adding leading zeros, make it at least as long as L
(define (zero-fill i L)
  (let ([n (number->string i)])
    (string-append (make-string (max (- L (string-length n)) 0) #\0) n)))

;; ---------------------------------------------------------------------------------------------------

;; MouseEvent% -> [List Nat Nat MouseEventType]
;; turn a mouse event into its pieces 
(define (mouse-event->parts e)
  (define x (- (send e get-x) INSET))
  (define y (- (send e get-y) INSET))
  (values x y 
          (cond [(send e button-down?) "button-down"]
                [(send e button-up?)   "button-up"]
                [(send e dragging?)    "drag"]
                [(send e moving?)      "move"]
                [(send e entering?)    "enter"]
                [(send e leaving?)     "leave"]
                [else ; (send e get-event-type)
                 (let ([m (send e get-event-type)])
                   (error 'on-mouse (format "Unknown event: ~a" m)))])))

;; KeyEvent% -> String
(define (key-event->parts e)
  (define x (send e get-key-code))
  (cond
    [(char? x) (string x)]
    [(symbol? x) (symbol->string x)]
    [else (error 'on-key (format "Unknown event: ~a" x))]))

;; KeyEvent% -> String
(define (key-release->parts e)
  (define x (send e get-key-release-code))
  (cond
    [(char? x) (string x)]
    [(symbol? x) (symbol->string x)]
    [else (error 'on-key (format "Unknown event: ~a" x))]))

;; ---------------------------------------------------------------------------------------------------
;; Any -> Symbol 
(define (name-of draw tag)
  (define fname  (object-name draw))
  (if fname fname tag))

;; ---------------------------------------------------------------------------------------------------
;; Any -> Boolean
(define (sexp? x)
  (cond
    [(empty? x) true]
    [(string? x) true]
    [(bytes? x) true]
    [(symbol? x) true]
    [(number? x) true]
    [(boolean? x) true]
    [(char? x) true]
    [(pair? x) (and (list? x) (andmap sexp? x))]
    [(and (struct? x) (prefab-struct-key x)) (for/and ((i (struct->vector x))) (sexp? i))]
    [else false]))

; tests:
;(struct s (t) #:prefab)
;(unless (sexp? (list (s (list 'a))))
;  (error 'prefab "structs should be sexp?"))

(define (no-newline? x)
  (not (member #\newline (string->list x))))

;; ---------------------------------------------------------------------------------------------------
;; exchange one-line messages between worlds and the server

(struct protocol (send    ;; OutPort Sexp -> Void
                  receive ;; InPort -> Sexp
                  ))
(define (make-protocol send receive)
  (protocol (λ (out msg) (send msg out) (flush-output out))
            (λ (in) (with-handlers ([exn? (lambda (x) (raise msgr-eof))])
                      (define ans (receive in))
                      (if (eof-object? ans)
                          (raise msgr-eof)
                          ans)))))
(struct msgr (exn:create ;; Any -> Boolean (check thrown exemption)
              creator    ;; Number -> A
              eventor    ;; A -> (Event (List In Out))
              connector  ;; Host Port -> (Values In Out)
              protocol   ;; protocol?
              ))

(define (make-tcp-msgr [max-wait 4] [reuse? #t] [hostname #f]
                       #:protocol [protocol default-protocol])
  (msgr exn:fail:network?
        (λ (port) (tcp-listen port max-wait reuse? hostname))
        tcp-accept-evt
        (λ (register port) (tcp-connect register port))
        protocol))


(define msgr-eof (gensym 'msgr-eof))

;; Any -> Boolean 
(define (msgr-eof? a) (eq? msgr-eof a))

;; OutPort Sexp -> Void
(define (default-send out msg)
  (write msg out)
  (newline out)
  (flush-output out))

;; InPort -> Sexp
(define (default-receive in)
  (with-handlers ((exn? (lambda (x) (raise msgr-eof))))
    (define x (read in))
    (if (eof-object? x) 
        (raise msgr-eof)
        (begin
          (read-line in) ;; read the newline 
          x))))

(define default-protocol (protocol default-send default-receive))
(define default-msgr (make-tcp-msgr))

(define (msgr-send MSGR) (protocol-send (msgr-protocol MSGR)))
(define (msgr-receive MSGR) (protocol-receive (msgr-protocol MSGR)))

;; msgr? InPort OutPort (X -> Y) -> (U Y Void)
;; process a registration from a potential client, invoke k on name if it is okay
(define (msgr-process-registration MSGR in out k)
  (define next ((msgr-receive MSGR) in))
  (println next)
  (match next
    [`(REGISTER ((name ,name)))
     ((msgr-send MSGR) out '(OKAY))
     (k name)]))
  
;; msgr? InPort OutPort (U #f String) -> Void 
;; register with the server, send the given name or make up a symbol 
(define (msgr-register MSGR in out name)
  (define msg `(REGISTER ((name ,(if name name (gensym 'world))))))
  ((msgr-send MSGR) out msg)
  (define ackn ((msgr-receive MSGR) in))
  (unless (equal? ackn '(OKAY))
    (raise msgr-eof)))

;                                                   
;                                                   
;                                                   
;    ;;;                         ;;;   ;      ;     
;   ;   ;                       ;   ;  ;      ;     
;   ;   ;                       ;   ;  ;      ;     
;   ;   ;  ; ;;    ;;;;         ;      ;;;;   ;  ;  
;   ;;;;;  ;;  ;  ;   ;         ;      ;   ;  ; ;   
;   ;   ;  ;   ;  ;   ;         ;      ;   ;  ;;    
;   ;   ;  ;      ;   ;         ;      ;   ;  ; ;   
;   ;   ;  ;      ;   ;         ;   ;  ;   ;  ;  ;  
;   ;   ;  ;       ;;;;          ;;;   ;   ;  ;   ; 
;                     ;                             
;                 ;   ;                             
;                  ;;;                              

;; Symbol Any String -> Void
(define (check-pos t c r)
  (check-arg 
   t (and (real? c) (>= (number->integer c t r) 0)) "positive integer" r c))
